package edu.berkeley.cs.succinct.perf.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctBuffer;
import edu.berkeley.cs.succinct.perf.BenchmarkUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SuccinctBufferBench {
    private static final int WARMUP_QUERIES = 10000;
    private static final int MAX_QUERIES = 100000;
    private SuccinctBuffer buffer;

    public SuccinctBufferBench(String serializedDataPath, StorageMode storageMode) {
        buffer = new SuccinctBuffer(serializedDataPath, storageMode);
    }

    public void benchLookupNPA(String resPath) throws IOException {
        System.out.println("Benchmarking lookupNPA...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize());

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        for(long i: randoms) {
            sum += buffer.lookupNPA(i);
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(long i: randoms) {
            long start = System.nanoTime();
            buffer.lookupNPA(i);
            long end = System.nanoTime();
            bufferedWriter.write(i + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / randoms.length;
        System.out.println("Average time per NPA lookup: " + avgTime);
        bufferedWriter.close();
    }

    public void benchLookupSA(String resPath) throws IOException {
        System.out.println("Benchmarking lookupSA...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize());

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        for(long i: randoms) {
            sum += buffer.lookupSA(i);
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(long i: randoms) {
            long start = System.nanoTime();
            buffer.lookupSA(i);
            long end = System.nanoTime();
            bufferedWriter.write(i + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / randoms.length;
        System.out.println("Average time per SA lookup: " + avgTime);
        bufferedWriter.close();
    }

    public void benchLookupISA(String resPath) throws IOException {
        System.out.println("Benchmarking lookupISA...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize());

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        long sum = 0, qCount = 0;
        for(long i: randoms) {
            sum += buffer.lookupISA(i);
            qCount++;
            if(qCount >= WARMUP_QUERIES) break;
        }

        System.out.println("Warmup complete: Checksum = " + sum);

        for(long i: randoms) {
            long start = System.nanoTime();
            buffer.lookupISA(i);
            long end = System.nanoTime();
            bufferedWriter.write(i + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / randoms.length;
        System.out.println("Average time per ISA lookup: " + avgTime);
        bufferedWriter.close();
    }

    public void benchAll(String resPath) throws IOException {
        benchLookupNPA(resPath + "_npa");
        benchLookupSA(resPath + "_sa");
        benchLookupISA(resPath + "_isa");
    }

}
