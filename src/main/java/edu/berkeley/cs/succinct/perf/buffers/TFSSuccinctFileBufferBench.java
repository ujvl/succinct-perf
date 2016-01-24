package edu.berkeley.cs.succinct.perf.buffers;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TFSSuccinctFileBufferBench extends SuccinctFileBufferBench {

    private static final String READ_TYPE = "NO_CACHE";
    private TachyonFileSystem tfs;

    public TFSSuccinctFileBufferBench(String tachyonMasterLoc, String tachyonFilePath, int threads, int extrLength) {

        super(null, threads, extrLength);

        setupTFS(tachyonMasterLoc);
        TachyonURI inFileURI = new TachyonURI(tachyonFilePath);

        ReadType rType = ReadType.valueOf(READ_TYPE);
        InStreamOptions readOptions = new InStreamOptions.Builder(ClientContext.getConf()).setReadType(rType).build();

        try {

            tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
            TachyonFile file = tfs.open(inFileURI);
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
        tachyonConf.set(Constants.MASTER_RPC_PORT, Integer.toString(masterLoc.getPort()));
        ClientContext.reset(tachyonConf);
    }

}