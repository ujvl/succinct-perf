package edu.berkeley.cs.succinct.perf.buffers;

import com.google.common.io.Closer;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.perf.TachyonUtil;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TFSSuccinctFileBufferBench extends SuccinctFileBufferBench {

    private static final String READ_TYPE = "NO_CACHE";
    private TachyonFileSystem tfs;
    private TachyonFile file;

    public TFSSuccinctFileBufferBench(String tachyonMasterLoc, String filePath) {

        super(null, "");
        TachyonUtil.setupTFS(tachyonMasterLoc);

        TachyonURI fileURI = new TachyonURI("/" + filePath);

        ReadType rType = ReadType.valueOf(READ_TYPE);
        InStreamOptions readOptions = new InStreamOptions.Builder(ClientContext.getConf()).setReadType(rType).build();

        try (Closer closer = Closer.create()) {

            tfs = TachyonFileSystem.TachyonFileSystemFactory.get();

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

            System.out.println("Reading tachyon file ByteBuffer...");
            ByteBuffer byteBuffer = TachyonUtil.readByteBuf(tfs, file, readOptions);

            //setFileBuffer(new SuccinctFileBuffer(byteBuffer));
            System.out.println("Done loading SuccinctFileBuffer!");

        } catch (TachyonException|IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }

}