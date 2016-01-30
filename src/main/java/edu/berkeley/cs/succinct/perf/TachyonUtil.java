package edu.berkeley.cs.succinct.perf;

import com.google.common.io.Closer;
import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class TachyonUtil {

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
