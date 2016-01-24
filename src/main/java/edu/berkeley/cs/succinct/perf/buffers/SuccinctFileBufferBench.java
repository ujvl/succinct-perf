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

    public SuccinctFileBufferBench(String serializedDataPath, StorageMode storageMode) {
        this(new SuccinctFileBuffer(serializedDataPath, storageMode), 1, 1000);
    }

    public SuccinctFileBufferBench(String serializedDataPath, StorageMode storageMode, int threads, int extrLen) {
        this(new SuccinctFileBuffer(serializedDataPath, storageMode), threads, extrLen);
    }

    public SuccinctFileBufferBench(SuccinctFileBuffer buf, int threads, int extrLen) {
        this.buffer = buf;
        NUM_THREADS = threads;
        EXTRACT_LENGTH = extrLen;
    }

    public void setFileBuffer(SuccinctFileBuffer buf) {
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

    public void benchSearchThroughput(String queryFile) throws IOException,
        InterruptedException, ExecutionException {

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

        for (Future<Integer> result : resAccumulator) {
            queriesExecuted += result.get();
        }

        System.out.println("Search queries executed per second: " + queriesExecuted/TOTAL_EXEC_TIME);
        executor.shutdown();

    }

    public void benchExtractThroughput() throws IOException,
        InterruptedException, ExecutionException {

        System.out.println("Benchmarking extract throughput with " + NUM_THREADS + " threads...");
        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize() - EXTRACT_LENGTH);
        int queriesExecuted = 0;

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        List<Future<Integer>> resAccumulator = new ArrayList<>(NUM_THREADS);
        Callable<Integer>[] benchTasks = new ExtractBenchTask[NUM_THREADS];

        for (int i = 0; i < NUM_THREADS; i++) {
            int offset = i/NUM_THREADS * randoms.length;
            benchTasks[i] = new ExtractBenchTask(randoms, offset);
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            resAccumulator.add(executor.submit(benchTasks[i]));
        }

        for (Future<Integer> result : resAccumulator) {
            queriesExecuted += result.get();
        }

        System.out.println("Extract queries executed per second: " + queriesExecuted/TOTAL_EXEC_TIME);
        executor.shutdown();

    }

    public void benchAll(String queryFile, String resPath) throws IOException, ExecutionException, InterruptedException {
        benchCountLatency(queryFile, resPath + "_count_lat");
        benchSearchLatency(queryFile, resPath + "_search_lat");
        benchExtractLatency(resPath + "_extract_lat");
        benchSearchThroughput(queryFile);
        benchExtractThroughput();
    }

    private class SearchBenchTask implements Callable<Integer> {

        private int queriesExecuted;
        private String[] queries;
        private int startOffset;

        public SearchBenchTask(String[] queries, int offset) {
            queriesExecuted = 0;
            this.queries = queries;
            startOffset = offset;
        }

        @Override
        public Integer call() {

            long until = System.currentTimeMillis() + WARMUP_TIME*1000L;

            int i = startOffset;
            while(System.currentTimeMillis() < until) {
                buffer.search(queries[i++].getBytes());
            }

            until = System.currentTimeMillis() + MEASUREMENT_TIME*1000L;
            while(System.currentTimeMillis() < until) {
                buffer.search(queries[i++].getBytes());
                queriesExecuted++;
            }

            until = System.currentTimeMillis() + COOLDOWN_TIME*1000L;
            while(System.currentTimeMillis() < until) {
                buffer.search(queries[i++].getBytes());
            }

            return queriesExecuted;
        }
    }

    private class ExtractBenchTask implements Callable<Integer> {

        private int queriesExecuted;
        private long[] randoms;
        private int startOffset;

        public ExtractBenchTask(long[] randoms, int offset) {
            queriesExecuted = 0;
            this.randoms = randoms;
            startOffset = offset;
        }

        @Override
        public Integer call() {

            long until = System.currentTimeMillis() + WARMUP_TIME*1000L;

            int i = startOffset;
            while(System.currentTimeMillis() < until) {
                buffer.extract(randoms[i++], EXTRACT_LENGTH);
            }

            until = System.currentTimeMillis() + MEASUREMENT_TIME*1000L;
            while(System.currentTimeMillis() < until) {
                buffer.extract(randoms[i++], EXTRACT_LENGTH);
                queriesExecuted++;
            }

            until = System.currentTimeMillis() + COOLDOWN_TIME*1000L;
            while(System.currentTimeMillis() < until) {
                buffer.extract(randoms[i++], EXTRACT_LENGTH);
            }

            return queriesExecuted;
        }
    }

}
