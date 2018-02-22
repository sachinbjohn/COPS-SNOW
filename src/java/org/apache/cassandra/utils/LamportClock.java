package org.apache.cassandra.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There should only be one LamportClock for each process, so this class is a
 * singleton (all static).
 *
 * @author wlloyd
 *
 */
public class LamportClock {
    //private static Logger logger = LoggerFactory.getLogger(LamportClock.class);

    //NO_CLOCK_TICK should only be used when calling function *locally* on the node
    public static final long NO_CLOCK_TICK = -1;

    //COPS_UNSUPPORTED should only be used in code that we don't intend to support like Hadoop on top of cassandra
    public static final long COPS_UNSUPPORTED = -2;

    private static AtomicLong logicalTime = new AtomicLong();
    private static Short localId = null;

    private LamportClock() {
        //don't instantiate me
    }

    //localId must be set before calling getVersion, otherwise you'll get a null exception

    /**
     * @return next "version" for this node, version is timestamp + nodeid
     */
    public static long getVersion() {
        long localTime = logicalTime.incrementAndGet();
        long version = (localTime << 16) + localId.shortValue();
        //logger.debug("getVersion {} = {} << 16 + {}", new Object[]{version, localTime, localId.shortValue()});
        return version;
    }

    //Should only be used for sanity checking
    public static long currentVersion() {
        return (logicalTime.get() << 16) + localId.shortValue();
    }


    public static long sendTimestamp() {
        long newLocalTime = logicalTime.incrementAndGet();
        //logger.debug("sendTimestamp({})", newLocalTime);
        return newLocalTime;
    }

    public static synchronized void updateTime(long updateTime) {
        if (updateTime == NO_CLOCK_TICK) {
            //logger.debug("updateTimestamp(NO_CLOCK_TICK == {})", updateTime);
            return;
        }

        long localTime = logicalTime.longValue();
        long timeDiff = updateTime - localTime;

        long resultTime;
        if (timeDiff < 0) {
            resultTime = logicalTime.incrementAndGet();
        } else {
            resultTime = logicalTime.addAndGet(timeDiff+1);
        }
        //logger.debug("updateTimestamp({},{}) = {}", new Object[]{updateTime, localTime, resultTime});
    }

    public static void setLocalId(short localId2) {
        localId = localId2;
    }

    //HL: add a function to return current lts
    public static long getCurrentTime() {
        return logicalTime.longValue();
    }

    //SNOW: for new way of generating txnid
    private static long parseToLong(String ipStr) throws Exception {
        if (ipStr.indexOf('.') < 0) {
            return 0L;
        }
        String[] strs = ipStr.split("\\.");
        //Using last 10 bits of ip address and 6 bits of thread. MAX of 64 threads per node
        long l10bits = (((Integer.parseInt(strs[2])%4) << 8 ) + Integer.parseInt(strs[3]));
        long uniqid = (l10bits << 6) + getThreadId();
        return uniqid;
    }

    // Atomic integer containing the next thread ID to be assigned
    private static final AtomicInteger nextId = new AtomicInteger(0);

    // Thread local variable containing each thread's ID
    private static final ThreadLocal<Integer> threadId =
            new ThreadLocal<Integer>() {
                @Override protected Integer initialValue() {
                    return nextId.getAndIncrement();
                }
            };

    // Returns the current thread's unique ID, assigning it if necessary
    public static int getThreadId() {
        return threadId.get();
    }

    public static long extractClientId(long txnid) {
        return txnid & ((1<<16)-1);
    }
    public static long sendTranId() throws Exception {
        long localTime = logicalTime.incrementAndGet();
        long tranId = (localTime << 16) + parseToLong(InetAddress.getLocalHost().getHostAddress());
        return tranId;
    }
}
