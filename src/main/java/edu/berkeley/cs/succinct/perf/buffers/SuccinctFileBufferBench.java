package edu.berkeley.cs.succinct.perf.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.perf.BenchmarkUtils;

import java.io.*;

public class SuccinctFileBufferBench {
    private static final int WARMUP_QUERIES = 10000;
    private static final int MAX_QUERIES = 100000;
    private SuccinctFileBuffer buffer;

    public SuccinctFileBufferBench(String serializedDataPath, StorageMode storageMode) {
        buffer = new SuccinctFileBuffer(serializedDataPath, storageMode);
    }

    public void benchCount(String queryFile, String resPath) throws IOException {
        System.out.println("Benchmarking count...");

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

    public void benchSearch(String queryFile, String resPath) throws IOException {
        System.out.println("Benchmarking search...");

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

    public void benchExtract(String resPath) throws IOException {
        System.out.println("Benchmarking extract...");

        int extractLength = 1000;
        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize() - extractLength);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        for(long offset: randoms) {
            sum += buffer.extract((int)offset, extractLength).length;
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(long offset: randoms) {
            long start = System.nanoTime();
            byte[] result = buffer.extract((int) offset, extractLength);
            long end = System.nanoTime();
            bufferedWriter.write(result.length + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / randoms.length;
        System.out.println("Average time per extract query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchAll(String queryFile, String resPath) throws IOException {
        benchCount(queryFile, resPath + "_count");
        benchSearch(queryFile, resPath + "_search");
        benchExtract(resPath + "_extract");

    }
}
