package org.apache.cassandra.db;

import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.service.IWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.LamportClock;
import org.apache.cassandra.utils.Pair;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by yangyang333 on 15-7-9.
 */
public class FetchIdCompletion implements ICompletable {
    private final List<Pair<RowMutation, IWriteResponseHandler>> rowMutations;
    private final HashMap<ByteBuffer, Set<Long>> uniqIds;
    public FetchIdCompletion(List<Pair<RowMutation, IWriteResponseHandler>> rms, HashMap<ByteBuffer, Set<Long>> uniqIds) {
        this.rowMutations = rms;
        this.uniqIds = uniqIds;
    }
    @Override
    public void complete() {
        for(Set<Long> sets : uniqIds.values()) {
            StorageProxy.numUniqIds.getAndAdd(sets.size());
            HashSet<Long> clients = new HashSet<>();
            for(Long id : sets) {
                Long clientId= LamportClock.extractClientId(id);
                clients.add(clientId);
            }
            StorageProxy.numUniqClients.getAndAdd(clients.size());
        }

        for (Pair<RowMutation, IWriteResponseHandler> rm : rowMutations) {
            StorageProxy.insertLocal(rm.left, rm.right);
        }
    }
}
