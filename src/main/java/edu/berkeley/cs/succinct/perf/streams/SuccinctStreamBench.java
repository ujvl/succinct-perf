package edu.berkeley.cs.succinct.perf.streams;

import edu.berkeley.cs.succinct.perf.BenchmarkUtils;
import edu.berkeley.cs.succinct.streams.SuccinctStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SuccinctStreamBench {
    private static final int MAX_QUERIES = 10000;
    private SuccinctStream buffer;

    public SuccinctStreamBench(String serializedDataPath) throws IOException {
        buffer = new SuccinctStream(new Path(serializedDataPath));
    }

    public void benchLookupNPA(String resPath) throws IOException {
        System.out.println("Benchmarking lookupNPA...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize());

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        for(long i: randoms) {
            long start = System.nanoTime();
            buffer.lookupNPA(i);
            long end = System.nanoTime();
            bufferedWriter.write(i + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / MAX_QUERIES;
        System.out.println("Average time per NPA lookup: " + avgTime);
        bufferedWriter.close();
    }

    public void benchLookupSA(String resPath) throws IOException {
        System.out.println("Benchmarking lookupSA...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize());

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        for(long i: randoms) {
            long start = System.nanoTime();
            buffer.lookupSA(i);
            long end = System.nanoTime();
            bufferedWriter.write(i + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / MAX_QUERIES;
        System.out.println("Average time per SA lookup: " + avgTime);
        bufferedWriter.close();
    }

    public void benchLookupISA(String resPath) throws IOException {
        System.out.println("Benchmarking lookupISA...");

        long[] randoms = BenchmarkUtils.generateRandoms(MAX_QUERIES, buffer.getOriginalSize());

        double totalTime = 0.0;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resPath));

        for(long i: randoms) {
            long start = System.nanoTime();
            buffer.lookupISA(i);
            long end = System.nanoTime();
            bufferedWriter.write(i + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / MAX_QUERIES;
        System.out.println("Average time per ISA lookup: " + avgTime);
        bufferedWriter.close();
    }

    public void benchAll(String resPath) throws IOException {
        benchLookupNPA(resPath + "_npa");
        benchLookupSA(resPath + "_sa");
        benchLookupISA(resPath + "_isa");
    }
}
