package org.apache.cassandra.db;

/**
 * HL: Created by Haonan on 15-2-1.
 */

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ReadTransactionIdTracker {
    public static final long SAFTYTIMER = 50;    // used for garbage collection of old versions, 50 milliseconds
    /**
     * keyToReadTxnIds contains all read transactions just partly happened on this node
     * It is a map of <locator_key, map<transaction_id, ArrayList<txnTime, record_time>>>.
     * So, those transaction ids are stored with locator_key each read txn touched.
     * record_time is used for garbage collection. txnTime is used for multiget_slice_by_time
     * We do garbage collection everytime we have a dep_check (seems reasonable)
     */
    static ConcurrentHashMap<ByteBuffer, ConcurrentHashMap<Long, ArrayList<Long>>> keyToReadTxnIds = new ConcurrentHashMap<ByteBuffer, ConcurrentHashMap<Long, ArrayList<Long>>>(100000);
    static ConcurrentHashMap<Long, Long> clientToMaxTxnId = new ConcurrentHashMap<>(2048);
    // Used to store last accessed time for each key. The first ROT set the access time for an empty
    // key entry, then after that only write's dep_check updates last accessed time. If a key' accessed time longer then
    // 5 seconds, Then a read will also trigger garbage collection.
    static ConcurrentHashMap<ByteBuffer, Long> keyToLastAccessedTime = new ConcurrentHashMap<ByteBuffer, Long>(100000);

    // method overloading
    public static void checkIfTxnIdBeenRecorded(ByteBuffer locatorKey, ArrayList<Long> txnIds, long chosenTime) {
        for (Long id: txnIds) {
            checkIfTxnIdBeenRecorded(locatorKey, id, true, chosenTime);
        }
    }
    /*
     * Check if this txn_id is recorded before (means current ROT need to return an old value)
     * If this txn_id is not in the record, then we add this id to the record.
     */
    public static long checkIfTxnIdBeenRecorded(ByteBuffer locatorKey, long txnId, boolean forWrites, long chosenTime)
    {
        Long transactionId = new Long(txnId);
        long txnId_clientId = LamportClock.extractClientId(txnId);
        if(!clientToMaxTxnId.contains(txnId_clientId) || txnId > clientToMaxTxnId.get(txnId_clientId))
            clientToMaxTxnId.put(txnId_clientId, txnId);
        long txnTimeToReturn = 0;
        //recordTime is real time for garbage collection
        Long recordTime = new Long(System.currentTimeMillis());
        Long txnTime = !forWrites ? new Long(0) : chosenTime;  // a place holder, txnTime should be filled in by writes after done dep_check
        //prepare time entry for this transaction. A time entry has transaction's effective time and record time
        ArrayList<Long> timesEntry = new ArrayList<Long>();
        timesEntry.add(txnTime);
        timesEntry.add(recordTime);
        ConcurrentHashMap<Long, ArrayList<Long>> txnIdList = keyToReadTxnIds.get(locatorKey);
        if (txnIdList == null) {
            //the locator_key is even not touched by other read txns yet
            ConcurrentHashMap<Long, ArrayList<Long>> txnEntry = new ConcurrentHashMap<Long, ArrayList<Long>>();
            txnEntry.put(transactionId, timesEntry);
            keyToLastAccessedTime.put(locatorKey, System.currentTimeMillis());
            keyToReadTxnIds.put(locatorKey, txnEntry);
            //            logger.trace("KeyRead "+ByteBufferUtil.bytesToHex(locatorKey));
//            txnEntry.clear();
//            txnEntry = null;
        }
        else {
            long safetyTime = System.currentTimeMillis() - SAFTYTIMER;
            // this key has not been checked by dep_check for a while, we need to explicitly do garbage collection
            if (keyToLastAccessedTime.get(locatorKey) < safetyTime) {
                for (Entry<Long, ArrayList<Long>> entry : txnIdList.entrySet()) {
                    Long oldId = entry.getKey();
                    Long oldId_client = LamportClock.extractClientId(oldId);
                    //SBJ: No need to maintain older transaction when newer transaction from same client exists
                    if (entry.getValue().get(1) < safetyTime || (clientToMaxTxnId.get(oldId_client) > oldId)) {
                        txnIdList.remove(oldId);
                    }
                }
                keyToLastAccessedTime.put(locatorKey, System.currentTimeMillis());
            }
            ArrayList<Long> findTxnId = txnIdList.get(transactionId);
            if (findTxnId == null) {
                // locator_key exists but this txnId is not in the record
                txnIdList.put(transactionId, timesEntry);
            } else {
                // if we did find this txnId recorded before, then we return its effective time

                // we took this assertion out, because if could happen txntime = 0, when a sub read arrived when dep_check flying
                //assert findTxnId.get(0) != 0 : "if we found a matched id, then its time must have been filled in by some writes";
                // if forWrites, to see if we need to update the txnTime, we keep the min of all txnTimes of this txnId
                txnTimeToReturn = findTxnId.get(0);
                if (forWrites) {
                    if (txnTimeToReturn > txnTime ) {
                        //SBJ: Causes NullPointerException possibly because another thread removed the transactionId  because of safety timen
                        // keyToReadTxnIds.get(locatorKey).get(transactionId).set(0, txnTime); //update txnTime
                        //Avoids NullPointer Exception, but if another thread removed txnid, this update is meaningless.
                        findTxnId.set(0, txnTime); //update txnTime
                    }
                }
//                findTxnId.clear();
//                findTxnId = null;
            }
//            txnIdList.clear();
//            txnIdList = null;
        }
        // if txnTimeToReturn not equal to 0, then it also means we found this txnId in our record
        return txnTimeToReturn;
    }

    //Return a list of txnIds associated with a locatorKey
    //Called by sendDepCheckReply to incorporate ROT ids
    public static ArrayList<Long> getReadTxnIds(ByteBuffer locatorKey) {
        ArrayList<Long> returnedIdList = new ArrayList<Long>();
        if (keyToReadTxnIds.get(locatorKey) == null)
            return returnedIdList;
        for (Entry<Long, ArrayList<Long>> entry : keyToReadTxnIds.get(locatorKey).entrySet()) {
            long safeTime = System.currentTimeMillis() - SAFTYTIMER;
            Long oldId = entry.getKey();
            Long oldId_client = LamportClock.extractClientId(oldId);
            //SBJ: Only adding latest transaction from each client
            if (entry.getValue().get(1) >= safeTime && clientToMaxTxnId.get(oldId_client).equals(oldId)) {
                returnedIdList.add(oldId);
            } else {
                keyToReadTxnIds.get(locatorKey).remove(oldId);
            }
        }
        keyToLastAccessedTime.put(locatorKey, System.currentTimeMillis());
        return returnedIdList;
    }

    //Clear stored txn Id map
    public static void clearContext() {
        keyToReadTxnIds.clear();
        keyToLastAccessedTime.clear();
    }
}
