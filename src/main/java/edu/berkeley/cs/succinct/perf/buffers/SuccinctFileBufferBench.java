package edu.berkeley.cs.succinct.perf.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.perf.BenchmarkUtils;

import java.io.*;

public class SuccinctFileBufferBench {
    private static final int MAX_QUERIES = 1000;
    private SuccinctFileBuffer buffer;

    public SuccinctFileBufferBench(String serializedDataPath, StorageMode storageMode) {
        buffer = new SuccinctFileBuffer(serializedDataPath, storageMode);
    }

    public void benchCount(String queryFile, String resPath) throws IOException {
        System.out.println("Benchmarking count...");

        String[] queries = BenchmarkUtils.readQueryFile(queryFile, MAX_QUERIES);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        for(String query: queries) {
            long start = System.nanoTime();
            long count = buffer.count(query.getBytes());
            long end = System.nanoTime();
            bufferedWriter.write(count + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / MAX_QUERIES;
        System.out.println("Average time per count query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchSearch(String queryFile, String resPath) throws IOException {
        System.out.println("Benchmarking search...");

        String[] queries = BenchmarkUtils.readQueryFile(queryFile, MAX_QUERIES);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        for(String query: queries) {
            long start = System.nanoTime();
            Long[] results = buffer.search(query.getBytes());
            long end = System.nanoTime();
            bufferedWriter.write(results.length + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / MAX_QUERIES;
        System.out.println("Average time per search query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchExtract(String resPath) throws IOException {
        System.out.println("Benchmarking extract...");

        int extractLength = 1000;
        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize() - extractLength);

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        for(long offset: randoms) {
            long start = System.nanoTime();
            byte[] result = buffer.extract((int) offset, extractLength);
            long end = System.nanoTime();
            bufferedWriter.write(result.length + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / MAX_QUERIES;
        System.out.println("Average time per extract query: " + avgTime);
        bufferedWriter.close();
    }

    public void benchAll(String queryFile, String resPath) throws IOException {
        benchCount(queryFile, resPath + "_count");
        benchSearch(queryFile, resPath + "_search");
        benchExtract(resPath + "_extract");

    }
}
