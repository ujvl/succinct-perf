package edu.berkeley.cs.succinct.perf.buffers;

import com.google.common.io.Closer;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.WriteType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class TFSSuccinctFileBufferBench extends SuccinctFileBufferBench {

    private static final String READ_TYPE = "NO_CACHE";
    private TachyonFileSystem tfs;

    public TFSSuccinctFileBufferBench(String tachyonMasterLoc, String filePath, int threads, int extrLength) {

        super(null, threads, extrLength);

        setupTFS(tachyonMasterLoc);

        TachyonURI fileURI = new TachyonURI("/" + filePath);
        File src = new File(filePath);

        try (Closer closer = Closer.create()) {
            FileOutStream os = closer.register(tfs.getOutStream(fileURI, OutStreamOptions.defaults()));
            FileInputStream in = closer.register(new FileInputStream(src));
            FileChannel channel = closer.register(in.getChannel());
            ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
            while (channel.read(buf) != -1) {
                buf.flip();
                os.write(buf.array(), 0, buf.limit());
            }
        } catch (IOException|TachyonException e) {
            e.printStackTrace();
        }

        ReadType rType = ReadType.valueOf(READ_TYPE);
        InStreamOptions readOptions = new InStreamOptions.Builder(ClientContext.getConf()).setReadType(rType).build();

        try {

            tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
            TachyonFile file = tfs.open(fileURI);
            ByteBuffer byteBuffer = readByteBuf(file, readOptions);

            setFileBuffer(new SuccinctFileBuffer(byteBuffer));

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