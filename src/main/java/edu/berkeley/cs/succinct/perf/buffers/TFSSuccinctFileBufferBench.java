package edu.berkeley.cs.succinct.perf.buffers;

import com.google.common.io.Closer;
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
import tachyon.exception.TachyonException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class TFSSuccinctFileBufferBench extends SuccinctFileBufferBench {

    private static final String READ_TYPE = "NO_CACHE";
    private TachyonFileSystem tfs;

    public TFSSuccinctFileBufferBench(String tachyonMasterLoc, String filePath) {

        super(null);
        setupTFS(tachyonMasterLoc);

        TachyonURI fileURI = new TachyonURI("/" + filePath);

        ReadType rType = ReadType.valueOf(READ_TYPE);
        InStreamOptions readOptions = new InStreamOptions.Builder(ClientContext.getConf()).setReadType(rType).build();

        try (Closer closer = Closer.create()) {

            tfs = TachyonFileSystem.TachyonFileSystemFactory.get();

            TachyonFile file = tfs.open(fileURI);
            if (file == null) {
                System.out.println("Copying file to tachyon...");
                File src = new File(filePath);
                FileOutStream os = closer.register(tfs.getOutStream(fileURI, OutStreamOptions.defaults()));
                FileInputStream in = closer.register(new FileInputStream(src));
                FileChannel channel = closer.register(in.getChannel());
                ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);

                while (channel.read(buf) != -1) {
                    buf.flip();
                    os.write(buf.array(), 0, buf.limit());
                }
            }

            System.out.println("Reading tachyon file byte buffer");
            ByteBuffer byteBuffer = readByteBuf(tfs.open(fileURI), readOptions);
            setFileBuffer(new SuccinctFileBuffer(byteBuffer));
            System.out.println("Done loading SuccinctFileBuffer!");

        } catch (TachyonException|IOException e) {
            e.printStackTrace();
            System.exit(-1);
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
    private ByteBuffer readByteBuf(TachyonFile file, InStreamOptions readOps) throws IOException, TachyonException {
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
    private void setupTFS(String masterURI) {
        TachyonURI masterLoc = new TachyonURI(masterURI);
        TachyonConf tachyonConf = ClientContext.getConf();
        tachyonConf.set(Constants.MASTER_HOSTNAME, masterLoc.getHost());
        tachyonConf.set(Constants.MASTER_PORT, Integer.toString(masterLoc.getPort()));
        ClientContext.reset(tachyonConf);
    }

}