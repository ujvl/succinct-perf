package edu.berkeley.cs.succinct.perf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

public class BenchmarkUtils {
    public static long[] generateRandoms(int numQueries, int limit) {
        long[] randoms = new long[numQueries];
        Random rand = new Random();
        for(int i = 0; i < numQueries; i++) {
            randoms[i] = rand.nextInt(limit);
        }
        return randoms;
    }

    public static String[] readQueryFile(String queryFile, int numQueries) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile));
        String[] queries = new String[numQueries];
        int i = 0;
        while((queries[i] = bufferedReader.readLine()) != null) i++;
        if(i < numQueries) {
            System.err.println("[WARNING] Number of queries is less then " + numQueries);
        }
        bufferedReader.close();
        return queries;
    }
}
