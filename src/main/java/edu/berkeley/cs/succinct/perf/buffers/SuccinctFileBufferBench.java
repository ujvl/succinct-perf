package edu.berkeley.cs.succinct.perf.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.perf.BenchmarkUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class SuccinctFileBufferBench {
    private static final int WARMUP_QUERIES = 10000;
    private static final int MAX_QUERIES = 100000;

    private static final int WARMUP_TIME = 300; // seconds
    private static final int COOLDOWN_TIME = 300; // seconds
    private static final int MEASUREMENT_TIME = 600; // seconds
    private static final int TOTAL_EXEC_TIME = WARMUP_TIME + MEASUREMENT_TIME + COOLDOWN_TIME;

    private final int NUM_THREADS;
    private final int EXTRACT_LENGTH;

    private SuccinctFileBuffer buffer;

    public SuccinctFileBufferBench() {
        buffer = new SuccinctFileBuffer();
        NUM_THREADS = 1;
        EXTRACT_LENGTH = 1000;
    }

    public SuccinctFileBufferBench(String serializedDataPath, StorageMode storageMode) {
        this();
        buffer = new SuccinctFileBuffer(serializedDataPath, storageMode);
    }

    public SuccinctFileBufferBench(String serializedDataPath, StorageMode storageMode, int threads, int extrLen) {
        buffer = new SuccinctFileBuffer(serializedDataPath, storageMode);
        NUM_THREADS = threads;
        EXTRACT_LENGTH = extrLen;
    }

    public void setSuccinctFileBuffer(SuccinctFileBuffer buf) {
        this.buffer = buf;
    }

    public void benchCountLatency(String queryFile, String resPath) throws IOException {
        System.out.println("Benchmarking count latency...");

        String[] queries = BenchmarkUtils.readQueryFile(queryFile, MAX_QUERIES);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        for(String query: queries) {
            sum += buffer.count(query.getBytes());
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(String query: queries) {
            byte[] queryBytes = query.getBytes();
            long start = System.nanoTime();
            long count = buffer.count(queryBytes);
            long end = System.nanoTime();
            bufferedWriter.write(count + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / queries.length;
        System.out.println("Average time per count query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchSearchLatency(String queryFile, String resPath) throws IOException {
        System.out.println("Benchmarking search latency...");

        String[] queries = BenchmarkUtils.readQueryFile(queryFile, MAX_QUERIES);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        for(String query: queries) {
            sum += buffer.search(query.getBytes()).length;
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(String query: queries) {
            byte[] queryBytes = query.getBytes();
            long start = System.nanoTime();
            Long[] results = buffer.search(queryBytes);
            long end = System.nanoTime();
            bufferedWriter.write(results.length + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / queries.length;
        System.out.println("Average time per search query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchExtractLatency(String resPath) throws IOException {
        System.out.println("Benchmarking extract latency...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize() - EXTRACT_LENGTH);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        for(long offset: randoms) {
            sum += buffer.extract((int) offset, EXTRACT_LENGTH).length;
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(long offset: randoms) {
            long start = System.nanoTime();
            byte[] result = buffer.extract((int) offset, EXTRACT_LENGTH);
            long end = System.nanoTime();
            bufferedWriter.write(result.length + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / randoms.length;
        System.out.println("Average time per extract query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchSearchThroughput(String queryFile, String resPath) throws IOException {
        System.out.println("Benchmarking search throughput with " + NUM_THREADS + " threads...");
        String[] queries = BenchmarkUtils.readQueryFile(queryFile, MAX_QUERIES);
        int queriesExecuted = 0;

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        List<Future<Integer>> resAccumulator = new ArrayList<>(NUM_THREADS);
        Callable<Integer>[] benchTasks = new SearchBenchTask[NUM_THREADS];

        for (int i = 0; i < NUM_THREADS; i++) {
            int offset = i/NUM_THREADS * queries.length;
            benchTasks[i] = new SearchBenchTask(queries, offset);
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            resAccumulator.add(executor.submit(benchTasks[i]));
        }

        try {
            for (Future<Integer> result : resAccumulator) {
                queriesExecuted += result.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return;
        }

        System.out.println("Queries executed per second: " + queriesExecuted/TOTAL_EXEC_TIME);
        executor.shutdown();

    }

    public void benchExtractThroughput(String resPath) throws IOException {
        System.out.println("Benchmarking extract throughput with " + NUM_THREADS + " threads...");
        //String[] queries = BenchmarkUtils.readQueryFile(queryFile, MAX_QUERIES);
        int queriesExecuted = 0;

        for (int i = 0; i < NUM_THREADS; i++) {

        }

    }

    public void benchAll(String queryFile, String resPath) throws IOException {
        benchCountLatency(queryFile, resPath + "_count");
        benchSearchLatency(queryFile, resPath + "_search");
        benchExtractLatency(resPath + "_extract");
    }

    private class SearchBenchTask implements Callable<Integer> {

        private int queriesExecuted;
        private String[] queries;

        public SearchBenchTask(String[] queries, int offset) {
            queriesExecuted = 0;
            this.queries = queries;
        }

        @Override
        public Integer call() {
            for (String query: queries) {
                buffer.search(query.getBytes());
                queriesExecuted++;
            }
            return queriesExecuted;
        }
    }

    private class ExtractBenchTask implements Callable<Integer> {

        private int queriesExecuted;
        private long[] randoms;

        public ExtractBenchTask(long[] randoms, int offset) {
            queriesExecuted = 0;
            this.randoms = randoms;
        }

        @Override
        public Integer call() {
            for (long offset: randoms) {
                buffer.extract(offset, EXTRACT_LENGTH);
                queriesExecuted++;
            }
            return queriesExecuted;
        }
    }

}
