package edu.berkeley.cs.succinct.perf;

import com.google.common.io.Closer;
import edu.berkeley.cs.succinct.buffers.SuccinctBuffer;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class TachyonUtil {

    private static final String READ_TYPE = "NO_CACHE";

    /**
     * Gets a SuccinctFileBuffer from the file at filePath stored on tfs
     * @param tachyonMasterLoc tachyon master address
     * @param filePath path of succinct file (no starting slash)
     * @return the SuccinctFileBuffer of the file
     */
    public static SuccinctFileBuffer getFileBuffer(String tachyonMasterLoc, String filePath) {

        TachyonFileSystem tfs;
        TachyonFile file;

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
                System.out.println("Copied file to tfs!\nTime taken: " + (stop-start)/1000 + " ms.");
                file = tfs.open(fileURI);
            }

            System.out.println("Reading tachyon file ByteBuffer...");
            ByteBuffer byteBuffer = TachyonUtil.readByteBuf(tfs, file, readOptions);

            return new SuccinctFileBuffer(byteBuffer);

        } catch (TachyonException|IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Gets a SuccinctBuffer from the file at filePath stored on tfs
     * @param tachyonMasterLoc tachyon master address
     * @param filePath path of succinct file (no starting slash)
     * @return the SuccinctBuffer of the file
     */
    public static SuccinctBuffer getBuffer(String tachyonMasterLoc, String filePath) {

        TachyonFileSystem tfs;
        TachyonFile file;

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
                System.out.println("Copied file to tfs!\nTime taken: " + (stop-start)/1000 + " ms.");
                file = tfs.open(fileURI);
            }

            System.out.println("Reading tachyon file ByteBuffer...");
            ByteBuffer byteBuffer = TachyonUtil.readByteBuf(tfs, file, readOptions);

            return new SuccinctFileBuffer(byteBuffer);

        } catch (TachyonException|IOException e) {
            e.printStackTrace();
            return null;
        }

    }


    /**
     * Copies file to tfs from inPath to outURI
     * @param inPath path of input file
     * @param outURI tachyon uri of output file
     * @param closer closer used in file copying
     * @throws IOException
     * @throws TachyonException
     */
    public static void copyFile(TachyonFileSystem tfs, String inPath, TachyonURI outURI, Closer closer)
        throws IOException, TachyonException {
        File src = new File(inPath);
        assert src.exists();
        OutStreamOptions.Builder opt = new OutStreamOptions.Builder(ClientContext.getConf());
        opt.setBlockSizeBytes(1610612736);
        FileOutStream os = closer.register(tfs.getOutStream(outURI, opt.build()));
        FileInputStream in = closer.register(new FileInputStream(src));
        FileChannel channel = closer.register(in.getChannel());
        ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
        while (channel.read(buf) != -1) {
            buf.flip();
            os.write(buf.array(), 0, buf.limit());
        }
    }

    /**
     * Reads ByteBuffer in from file existing in tfs
     * @param file file to read from
     * @param readOps read options
     * @return byte buffer of file
     * @throws IOException
     * @throws TachyonException
     */
    public static ByteBuffer readByteBuf(TachyonFileSystem tfs, TachyonFile file, InStreamOptions readOps)
        throws IOException, TachyonException {
        FileInStream inStream = tfs.getInStream(file, readOps);
        ByteBuffer buf = ByteBuffer.allocate((int) inStream.remaining());
        inStream.read(buf.array());
        buf.order(ByteOrder.BIG_ENDIAN);
        return buf;
    }

    /**
     * Sets up the tfs configuration
     * @param masterURI master URI of tfs instance
     */
    public static void setupTFS(String masterURI) {
        TachyonURI masterLoc = new TachyonURI(masterURI);
        TachyonConf tachyonConf = ClientContext.getConf();
        tachyonConf.set(Constants.MASTER_HOSTNAME, masterLoc.getHost());
        tachyonConf.set(Constants.MASTER_PORT, Integer.toString(masterLoc.getPort()));
        ClientContext.reset(tachyonConf);
    }

}
