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
    private static final int MAX_THR_EXT_QUERIES = 10000000;

    private static final int WARMUP_TIME = 300; // seconds
    private static final int COOLDOWN_TIME = 300; // seconds
    private static final int MEASUREMENT_TIME = 600; // seconds

    private SuccinctFileBuffer buffer;

    public SuccinctFileBufferBench(String serializedDataPath, StorageMode storageMode) {
        this(new SuccinctFileBuffer(serializedDataPath, storageMode));
    }

    public SuccinctFileBufferBench(SuccinctFileBuffer buf) {
        this.buffer = buf;
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

    public void benchExtractLatency(String resPath, int extrLen) throws IOException {
        System.out.println("Benchmarking extract latency...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize() - extrLen);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        for(long offset: randoms) {
            sum += buffer.extract((int) offset, extrLen).length;
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(long offset: randoms) {
            long start = System.nanoTime();
            byte[] result = buffer.extract((int) offset, extrLen);
            long end = System.nanoTime();
            bufferedWriter.write(new String(result) + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / randoms.length;
        System.out.println("Average time per extract query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchSearchThroughput(String queryFile, int numThreads) throws IOException,
        InterruptedException, ExecutionException {

        System.out.println("Benchmarking search throughput with " + numThreads + " threads...");
        String[] queries = BenchmarkUtils.readQueryFile(queryFile, MAX_QUERIES);
        int queriesExecuted = 0;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Integer>> resAccumulator = new ArrayList<>(numThreads);
        Callable<Integer>[] benchTasks = new SearchBenchTask[numThreads];

        for (int i = 0; i < numThreads; i++) {
            int offset = i/numThreads * queries.length;
            benchTasks[i] = new SearchBenchTask(queries, offset);
        }

        for (int i = 0; i < numThreads; i++) {
            resAccumulator.add(executor.submit(benchTasks[i]));
        }

        for (Future<Integer> result : resAccumulator) {
            queriesExecuted += result.get();
        }

        System.out.println("Search queries executed per second: " + queriesExecuted/MEASUREMENT_TIME);
        executor.shutdown();

    }

    public void benchExtractThroughput(int extrLen, int numThreads) throws IOException,
        InterruptedException, ExecutionException {

        System.out.println("Benchmarking extract throughput with " + numThreads + " threads...");
        long[] randoms = BenchmarkUtils.generateRandoms(MAX_THR_EXT_QUERIES, buffer.getOriginalSize() - extrLen);
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

    public void benchAll(String queryFile, String resPath, int threads, int extrLength)
        throws IOException, ExecutionException, InterruptedException {
        benchAllLatency(queryFile, resPath, extrLength);
        benchAllThroughput(queryFile, extrLength, threads);
    }

    public void benchAllLatency(String queryFile, String resPath, int extrLength) throws IOException {
        benchCountLatency(queryFile, resPath + "_count_lat");
        benchSearchLatency(queryFile, resPath + "_search_lat");
        benchExtractLatency(resPath + "_extract_lat", extrLength);
    }

    public void benchAllThroughput(String queryFile, int extrLength, int threads)
        throws IOException, ExecutionException, InterruptedException {
        benchSearchThroughput(queryFile, threads);
        benchExtractThroughput(extrLength, threads);
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

            while(System.currentTimeMillis() < until) {
                buffer.extract(randoms[i++], extrLen);
            }

            until = System.currentTimeMillis() + MEASUREMENT_TIME*1000L;
            while(System.currentTimeMillis() < until) {
                buffer.extract(randoms[i++], extrLen);
                queriesExecuted++;
            }

            until = System.currentTimeMillis() + COOLDOWN_TIME*1000L;
            while(System.currentTimeMillis() < until) {
                buffer.extract(randoms[i++], extrLen);
            }

            return queriesExecuted;
        }
    }

}
