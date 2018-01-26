/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stress;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.cassandra.client.ClientContext;
import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.stress.operations.*;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.Cassandra;

import static com.google.common.base.Charsets.UTF_8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StressAction extends Thread
{
    private static Logger logger = LoggerFactory.getLogger(StressAction.class);
    /**
     * Producer-Consumer model: 1 producer, N consumers
     */
    private final BlockingQueue<Operation> operations = new SynchronousQueue<Operation>(true);

    private final Session client;
    private final PrintStream output;
    private final ClientContext clientContext;

    private volatile boolean stop = false;

    private final int key_sets_size;   // for pre-generate 1000000 key sets = 5000000 keys

    private static Vector< List<ByteBuffer> > read_key_sets, write_key_sets;

    public StressAction(Session session, PrintStream out, ClientContext clientContext)
    {
        client = session;
        output = out;
        this.clientContext = clientContext;
        if (client.getKeys_per_read() != 0) {
            this.key_sets_size = 5000000 / client.getKeys_per_read();
        } else {
            this.key_sets_size = 5000000;   // for population, it doesn't matter
        }
    }

    @Override
    public void run()
    {
        long latency, oldLatency;
        int epoch, total, oldTotal, keyCount, oldKeyCount;
        int columnCount, oldColumnCount;
        long byteCount, oldByteCount;

        // creating keyspace and column families
        if (client.getOperation() == Stress.Operations.INSERT || client.getOperation() == Stress.Operations.COUNTER_ADD || client.getOperation() == Stress.Operations.INSERTCL || client.getOperation() == Stress.Operations.FACEBOOK_POPULATE)
            client.createKeySpaces();

	 // pre-generate keys before produce-consume
        if (client.getOperation() == Stress.Operations.DYNAMIC && client.useZipfianGenerator()) {
            read_key_sets = new Vector<List<ByteBuffer>>();
            write_key_sets = new Vector<List<ByteBuffer>>();
            try {
 output.println("pre-generating zipfian keys");
                pre_generate_keys();
            } catch (IOException e) {}
            assert(read_key_sets.size() == key_sets_size && write_key_sets.size() == key_sets_size);
 output.println("pre-genrating keys is done!!!"); 
       }

        int threadCount = client.getThreads();
        Consumer[] consumers = new Consumer[threadCount];

        int itemsPerThread = client.getKeysPerThread();
        int modulo = client.getNumKeys() % threadCount;

        // creating required type of the threads for the test
        for (int i = 0; i < threadCount; i++) {
            if (i == threadCount - 1)
                itemsPerThread += modulo; // last one is going to handle N + modulo items

            consumers[i] = new Consumer(itemsPerThread);
        }

        Producer producer = new Producer();
        // Wait until all clients are up
        boolean isExp10 = client.getOperation() == Stress.Operations.EXP10;
        if(isExp10) {
            try {
                new ClientSyncer(client, -1, output).run(client.getClientLibrary());
            } catch (Exception e) {
                logger.error("ClientSyncer has exception", e);
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        }

        producer.start();

        // starting worker threads
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

        // initialization of the values
        boolean terminate = false;
        latency = byteCount = 0;
        epoch = total = keyCount = columnCount = 0;

        int interval = client.getProgressInterval();
        int epochIntervals = client.getProgressInterval() * 10;
        long testStartTime = System.currentTimeMillis();

	Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
	    @Override
		public void run() {
                stop=true;
                printLatencyPercentiles();
            }
	    }));
        boolean before=true,after=false;
        while (!terminate)
        {
            if (before && client.exptDurationMs > client.warmupPeriodSeconds * 1000) {
                client.measureStats = true;
                before = false;
            }
            if (!after && client.exptDurationMs > (client.warmupPeriodSeconds + client.specifiedExptDurationSeconds) * 1000) {
                client.measureStats = false;
                after = true;
            }
            if (stop || (isExp10 && client.exptDurationMs > (client.specifiedExptDurationSeconds+2*client.warmupPeriodSeconds) * 1000))
            {
                producer.stopProducer();

                for (Consumer consumer : consumers)
                    consumer.stopConsume();
                client.exptDurationMs -= 2*client.warmupPeriodSeconds*1000;
                break;
            }

            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e.getMessage(), e);
            }

            int alive = 0;
            for (Thread thread : consumers)
                if (thread.isAlive()) alive++;

            if (alive == 0)
                terminate = true;

            epoch++;

            if (terminate || epoch > epochIntervals)
            {
                epoch = 0;

                oldTotal = total;
                oldLatency = latency;
                oldKeyCount = keyCount;
                oldColumnCount = columnCount;
                oldByteCount = byteCount;

                total = client.operations.get();
                keyCount = client.keys.get();
                columnCount = client.columnCount.get();
                byteCount = client.bytes.get();
                latency = client.latency.get();

                int opDelta = total - oldTotal;
                int keyDelta = keyCount - oldKeyCount;
                int columnDelta = columnCount - oldColumnCount;
                long byteDelta = byteCount - oldByteCount;
                double latencyDelta = latency - oldLatency;

                client.exptDurationMs = System.currentTimeMillis() - testStartTime;
                long currentTimeInSeconds = client.exptDurationMs / 1000;
                String formattedDelta = (opDelta > 0) ? Double.toString(latencyDelta / (opDelta * 1000)) : "NaN";

                output.println(String.format("Alive= %d,%d,%d,%d,%d,%d,%s,%d", alive, total, opDelta / interval, keyDelta / interval, columnDelta / interval, byteDelta / interval, formattedDelta, currentTimeInSeconds));
            }
        }

        // marking an end of the output to the client
        output.println("END");
    }

    protected byte[] generateZipfianKey(ZipfianGenerator zipfian, int size)
    {
        String format = "%0" + client.getTotalKeysLength() + "d";
        return String.format(format, zipfian.nextInt(size)).getBytes(UTF_8);
    }
    // pre-generate key_sets. key_sets is a vector of list of keys.
    // Each key_set has n keys, where n is number of keys per txn
    public void pre_generate_keys() throws IOException {

        double zipfianConstant = client.getZipfianConstant();
        int numKeys_read = client.getKeys_per_read(),
            numKeys_write = client.getKeys_per_write();
        // generate read keys
        ZipfianGenerator zipfian = new ZipfianGenerator(0, Stress.session.getNumDifferentKeys() - 1, zipfianConstant);
        for (int x = 0; x < key_sets_size; ++x) {
            List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
            for (int i = 0; keys.size() < numKeys_read; i++)
            {
                ByteBuffer newKey;
                newKey = ByteBuffer.wrap(generateZipfianKey(zipfian, Stress.session.getNumDifferentKeys()));
                if (!keys.contains(newKey)) {
                    keys.add(newKey);
                }
            }
            if (keys.size() != numKeys_read) {
                error("Pre_generating, could not generate enough unique keys for read, " + keys.size() + " instead of " + numKeys_read);
            }
            read_key_sets.add(keys);
        }
        // generate write keys
        ZipfianGenerator zipfian_write = new ZipfianGenerator(0, Stress.session.getNumDifferentKeys() - 1, zipfianConstant);
        for (int x = 0; x < key_sets_size; ++x) {
            List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
            for (int i = 0; keys.size() < numKeys_write; i++)
            {
                ByteBuffer newKey;
                newKey = ByteBuffer.wrap(generateZipfianKey(zipfian_write, Stress.session.getNumDifferentKeys()));
                if (!keys.contains(newKey)) {
                    keys.add(newKey);
                }
            }
            if (keys.size() != numKeys_write) {
                error("Pre_generating, could not generate enough unique keys for write, " + keys.size() + " instead of " + numKeys_write);
            }
            write_key_sets.add(keys);
        }

    }

    private Long mean(Long[] array) {
        if(array.length == 0)
            return  -1L;
        long sum = 0;
        for(int i = 0; i < array.length; ++i)
            sum += array[i];
        return sum/array.length;
    }


    private Long percentile(Long[] array, double percentile)
    {
        if(array.length == 0)
            return -1L;
        return array[(int) (array.length * (percentile / 100))];

    }

    private void printLatencyPercentiles() {

        if(client.latencies.size() == 0)  // We aren't recording latencies for this op type probably
            return;

        Long[] readlatencies = client.readlatencies.toArray(new Long[0]);
        Long[] writelatencies = client.writelatencies.toArray(new Long[0]);
        Arrays.sort(readlatencies);
        Arrays.sort(writelatencies);
        /*
        Expt,Key/Serv,#Serv,ValSize,Key/Read,WriteFrac,Zipf,Threads,Client,NumOps,NumKeys,NumColumns,NumBytes,NUmReads,NumWrites,Duration,Throughput,Ravg,R50,R90,R99,Wavg,W50,W90,W99,#Tx2R,#K2R,#aggR,#aggW,Lsum,Lavg
         */

        int numReads = client.numReads.get();
        int numWrites = client.numWrites.get();
        int numOps = client.operations.get();
        int numKeys = client.keys.get();
        int numColumns = client.columnCount.get();
        long numBytes = client.bytes.get();
        long duration = client.exptDurationMs;
        int num2RoundTxn = 0;
        int num2RoundKey = 0;
        int nR = readlatencies.length; //aggregated reads
        int nW = writelatencies.length; //aggregared writes
        long latency = client.latency.get();

        //Expt,Key/Serv,#Serv,ValSize,Key/Read,WriteFrac,Zipf,Threads,Client
        ArrayList<String> outputs = new ArrayList<>();
        outputs.add("COPS-SNOW");
        outputs.add(String.valueOf(client.getKeys_per_server()));
        outputs.add(String.valueOf(client.getNum_servers()));
        outputs.add(String.valueOf(client.getColumnSize()));
        outputs.add(String.valueOf(client.getKeys_per_read()));
        outputs.add(String.valueOf(client.getWrite_fraction()));
        outputs.add(String.valueOf(client.getZipfianConstant()));
        outputs.add(String.valueOf(client.getThreads()));
        outputs.add("Client"+client.stressIndex);

        //NumOps,NumKeys,NumColumns,NumBytes,NUmReads,NumWrites,Duration,Throughput
        outputs.add(String.valueOf(numOps));
        outputs.add(String.valueOf(numKeys));
        outputs.add(String.valueOf(numColumns));
        outputs.add(String.valueOf(numBytes));
        outputs.add(String.valueOf(numReads));
        outputs.add(String.valueOf(numWrites));
        outputs.add(String.valueOf(duration));
        outputs.add(String.valueOf(numOps*1000/duration));

        //Ravg,R50,R90,R99
        outputs.add(String.valueOf(mean(readlatencies)));
        outputs.add(String.valueOf(percentile(readlatencies,50)));
        outputs.add(String.valueOf(percentile(readlatencies,90)));
        outputs.add(String.valueOf(percentile(readlatencies,99)));

        //Wavg,W50,W90,W99
        outputs.add(String.valueOf(mean(writelatencies)));
        outputs.add(String.valueOf(percentile(writelatencies,50)));
        outputs.add(String.valueOf(percentile(writelatencies,90)));
        outputs.add(String.valueOf(percentile(writelatencies,99)));

        //#Tx2R,#K2R,
        outputs.add(String.valueOf(num2RoundTxn));
        outputs.add(String.valueOf(num2RoundKey));

        //#aggR,#aggW
        outputs.add(String.valueOf(nR));
        outputs.add(String.valueOf(nW));

        //Lsum, Lavg
        outputs.add(String.valueOf(latency));
        outputs.add(String.valueOf(latency*1000/numOps));
        System.err.println(String.join(",",outputs));

    }

    /**
     * Produces exactly N items (awaits each to be consumed)
     */
    private class Producer extends Thread
    {
        private volatile boolean stop = false;

        @Override
        public void run()
        {
            for (int i = 0; i < client.getNumKeys(); i++)
            {
                if (stop)
                    break;

                try {
                    operations.put(createOperation((i % client.getNumDifferentKeys()) + client.getKeysOffset()));
                } catch (InterruptedException e) {
                    logger.error("Producer error", e);
                    System.err.println("Producer error - " + e.getMessage());
                    return;
                }
            }
        }

        public void stopProducer()
        {
            stop = true;
        }
    }

    /**
     * Each consumes exactly N items from queue
     */
    private class Consumer extends Thread
    {
        private final int items;
        private volatile boolean stop = false;

        public Consumer(int toConsume)
        {
            items = toConsume;
        }

        @Override
        public void run() {
            try {
                if (client.getOperation() == Stress.Operations.DYNAMIC ||
                        client.getOperation() == Stress.Operations.INSERTCL ||
                        client.getOperation() == Stress.Operations.FACEBOOK ||
                        client.getOperation() == Stress.Operations.FACEBOOK_POPULATE ||
                        client.getOperation() == Stress.Operations.WRITE_TXN ||
                        client.getOperation() == Stress.Operations.BATCH_MUTATE ||
                        client.getOperation() == Stress.Operations.TWO_ROUND_READ_TXN ||
                        client.getOperation() == Stress.Operations.DYNAMIC_ONE_SERVER ||
                        client.getOperation() == Stress.Operations.EXP10) {
                    ClientLibrary library = client.getClientLibrary();

                    for (int i = 0; i < items; i++) {
                        if (stop)
                            break;

                        try {
                            operations.take().run(library); // running job
                        } catch (Exception e) {
                            logger.error("Consumer encountered error in operation", e);
                        }

                    }
                } else {
                    Cassandra.Client connection = client.getClient();

                    for (int i = 0; i < items; i++) {
                        if (stop)
                            break;

                        try {
                            operations.take().run(connection); // running job
                        } catch (Exception e) {
                            if (output == null) {
                                System.err.println(e.getMessage());
                                e.printStackTrace();
                                System.exit(-1);
                            }


                            output.println(e.getMessage());
                            e.printStackTrace();
                            break;
                        }
                    }
                }
            } catch (Throwable e) {
                logger.error("Consumer has error", e);
            }
        }

        public void stopConsume()
        {
            stop = true;
        }
    }

    private Operation createOperation(int index)
    {
        switch (client.getOperation())
        {
            case READ:
                return client.isCQL() ? new CqlReader(client, index) : new Reader(client, index, clientContext);

            case COUNTER_GET:
                return client.isCQL() ? new CqlCounterGetter(client, index) : new CounterGetter(client, index, clientContext);

            case INSERT:
                return client.isCQL() ? new CqlInserter(client, index) : new Inserter(client, index);

            case COUNTER_ADD:
                return client.isCQL() ? new CqlCounterAdder(client, index) : new CounterAdder(client, index, clientContext);

            case RANGE_SLICE:
                return client.isCQL() ? new CqlRangeSlicer(client, index) : new RangeSlicer(client, index, clientContext);

            case INDEXED_RANGE_SLICE:
                return client.isCQL() ? new CqlIndexedRangeSlicer(client, index) : new IndexedRangeSlicer(client, index, clientContext);

            case MULTI_GET:
                return client.isCQL() ? new CqlMultiGetter(client, index) : new MultiGetter(client, index, clientContext);

            case DYNAMIC:
                if (client.isCQL())
                    throw new RuntimeException("CQL not supprted with dynamic workload");
                return new DynamicWorkload(client, index, read_key_sets, write_key_sets, key_sets_size);

            case DYNAMIC_ONE_SERVER:
                if (client.isCQL())
                    throw new RuntimeException("CQL not supprted with dynamic workload");
                return new DynamicOneServer(client, index);

            case INSERTCL:
                return client.isCQL() ? new CqlInserter(client, index) : new Inserter(client, index);

            case EXP10:
                if (client.isCQL())
                    throw new RuntimeException("CQL is not supported with Exp10 workload");
                return new Experiment10(client, index);

	    case WRITE_TXN:
		if (client.isCQL())
		    throw new RuntimeException("CQL not support with write txn workload");
                return new WriteTransactionWorkload(client, index, true);

            case BATCH_MUTATE:
                if (client.isCQL())
                    throw new RuntimeException("CQL not support with write txn workload");
                return new WriteTransactionWorkload(client, index, false);

            case TWO_ROUND_READ_TXN:
                if (client.isCQL())
                    throw new RuntimeException("CQL not support with this workload");
                return new TwoRoundReadTxn(client, index);

	    case FACEBOOK_POPULATE:
		if (client.isCQL())
                    throw new RuntimeException("CQL not support with this workload");
		return new FacebookPopulator(client, index);

	    case FACEBOOK:
		if (client.isCQL())
                    throw new RuntimeException("CQL not support with this workload");
		return new FacebookWorkload(client, index);
        }

        throw new UnsupportedOperationException();
    }

    public void stopAction()
    {
        stop = true;
    }

    protected void error(String message) throws IOException
    {
        if (!client.ignoreErrors())
            throw new IOException(message);
        else
            System.err.println(message);
    }
}
