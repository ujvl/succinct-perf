package edu.berkeley.cs.succinct.perf;

import com.google.common.io.Closer;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class VanillaTachyonBench {

    private static final String READ_TYPE = "NO_CACHE";
    private static final int MAX_QUERIES = 100000;
    private static final int WARMUP_QUERIES = 10000;
    private static final int MAX_THR_EXT_QUERIES = 100000000;

    private static final int WARMUP_TIME = 300; // seconds
    private static final int COOLDOWN_TIME = 300; // seconds
    private static final int MEASUREMENT_TIME = 600; // seconds

    private TachyonFileSystem tfs;
    private ByteBuffer buf;
    private long numBytes;

    public VanillaTachyonBench(String tachyonMasterLoc, String filePath) {

        TachyonUtil.setupTFS(tachyonMasterLoc);

        TachyonURI fileURI = new TachyonURI("/" + filePath);

        ReadType rType = ReadType.valueOf(READ_TYPE);
        InStreamOptions readOptions = new InStreamOptions.Builder(ClientContext.getConf()).setReadType(rType).build();

        try (Closer closer = Closer.create()) {

            tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
            TachyonFile file;

            try {
                file = tfs.open(fileURI);
            } catch (InvalidPathException e) {
                System.out.println("File does not exist on tfs. Attempting to copy file from local to tfs...");
                long start = System.currentTimeMillis();
                TachyonUtil.copyFile(tfs, filePath, fileURI, closer);
                long stop = System.currentTimeMillis();
                System.out.println("Copied file to tfs!\nTime taken: " + (stop-start)/1000);
                file = tfs.open(fileURI);
            }

            FileInStream inStream = tfs.getInStream(file, readOptions);
            numBytes = inStream.remaining();
            System.out.println("[Sanity check] Number of bytes in file: " + numBytes);

            System.out.println("Reading tachyon file ByteBuffer...");
            buf = TachyonUtil.readByteBuf(tfs, file, readOptions);
            System.out.println("Done reading ByteBuffer!");

        } catch (TachyonException |IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }

    public void benchAll(String resPath, int extrLen, int numThreads)
        throws IOException, InterruptedException, ExecutionException {
        benchExtractLatency(resPath, extrLen);
        benchExtractThroughput(extrLen, numThreads);
    }

    public void benchExtractLatency(String resPath, int extrLen) throws IOException {
        System.out.println("Benchmarking extract latency...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, (int) numBytes - extrLen);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        byte[] result = new byte[extrLen];
        for(long offset: randoms) {
            for (int i = 0; i < extrLen; i++) {
                result[i] = buf.get((int) offset + i);
            }
            sum += extrLen;
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(long offset: randoms) {
            long start = System.nanoTime();
            for (int i = 0; i < extrLen; i++) {
                result[i] = buf.get((int) offset + i);
            }
            long end = System.nanoTime();
            bufferedWriter.write(new String(result) + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / randoms.length;
        System.out.println("Average time per extract query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchExtractThroughput(int extrLen, int numThreads) throws IOException,
        InterruptedException, ExecutionException {

        System.out.println("Benchmarking extract throughput with " + numThreads + " threads...");
        long[] randoms = BenchmarkUtils.generateRandoms(MAX_THR_EXT_QUERIES, (int) numBytes - extrLen);
        System.out.println("Generated " + MAX_THR_EXT_QUERIES + " extract queries. Starting benchmark...");
        int queriesExecuted = 0;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Integer>> resAccumulator = new ArrayList<>(numThreads);
        Callable<Integer>[] benchTasks = new ExtractBenchTask[numThreads];

        for (int i = 0; i < numThreads; i++) {
            int offset = i/numThreads * randoms.length;
            benchTasks[i] = new ExtractBenchTask(randoms, extrLen, offset);
        }

        for (int i = 0; i < numThreads; i++) {
            resAccumulator.add(executor.submit(benchTasks[i]));
        }

        for (Future<Integer> result : resAccumulator) {
            queriesExecuted += result.get();
        }

        System.out.println("Extract queries executed per second: " + queriesExecuted/MEASUREMENT_TIME);
        executor.shutdown();

    }

    private class ExtractBenchTask implements Callable<Integer> {

        private int queriesExecuted, startOffset, extrLen;
        private long[] randoms;

        public ExtractBenchTask(long[] randoms, int extrLen, int offset) {
            queriesExecuted = 0;
            this.randoms = randoms;
            this.extrLen = extrLen;
            startOffset = offset;
        }

        @Override
        public Integer call() {

            long until = System.currentTimeMillis() + WARMUP_TIME*1000L;
            int i = startOffset;
            int lim = randoms.length;

            byte[] result = new byte[extrLen];

            while(System.currentTimeMillis() < until) {
                for (int j = 0; j < extrLen; j++) {
                    result[j] = buf.get((int) randoms[i++] + j);
                }
                if (i == lim) {
                    i = 0;
                }
            }

            until = System.currentTimeMillis() + MEASUREMENT_TIME*1000L;
            while(System.currentTimeMillis() < until) {
                for (int j = 0; j < extrLen; j++) {
                    result[j] = buf.get((int) randoms[i++] + j);
                }
                if (i == lim) {
                    i = 0;
                }
                queriesExecuted++;
            }

            until = System.currentTimeMillis() + COOLDOWN_TIME*1000L;
            while(System.currentTimeMillis() < until) {
                for (int j = 0; j < extrLen; j++) {
                    result[j] = buf.get((int) randoms[i++] + j);
                }
                if (i == lim) {
                    i = 0;
                }
            }

            return queriesExecuted;
        }
    }

}
