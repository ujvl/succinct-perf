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

public class VanillaTachyonBench {

    private static final String READ_TYPE = "NO_CACHE";
    private static final int MAX_QUERIES = 100000;
    private static final int WARMUP_QUERIES = 10000;
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

    public void benchAll(String resPath, int extrLen) throws IOException {
        benchExtractLatency(resPath, extrLen);
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
                result[i] = buf.get((int) offset);
            }
            long end = System.nanoTime();
            bufferedWriter.write(new String(result) + "\t" + (end - start) + "\n");
            totalTime += (end - start);
        }

        double avgTime = totalTime / randoms.length;
        System.out.println("Average time per extract query: " + avgTime);
        bufferedWriter.close();
    }

}
