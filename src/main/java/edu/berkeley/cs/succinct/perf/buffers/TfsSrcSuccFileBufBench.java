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

public class TfsSrcSuccFileBufBench extends SuccinctFileBufferBench {

    private static final String READ_TYPE = "NO_CACHE";

    public TfsSrcSuccFileBufBench(String tachyonMasterLoc, String tachyonFilePath) {

        setupTFS(tachyonMasterLoc);
        TachyonURI inFileURI = new TachyonURI(tachyonFilePath);

        ReadType rType = ReadType.valueOf(READ_TYPE);
        InStreamOptions readOptions = new InStreamOptions.Builder(ClientContext.getConf()).setReadType(rType).build();

        try {

            TachyonFileSystem tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
            TachyonFile file = tfs.open(inFileURI);
            ByteBuffer byteBuffer = readByteBuf(tfs, file, readOptions);

            setSuccinctFileBuffer(new SuccinctFileBuffer(byteBuffer));

        } catch (TachyonException|IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void benchAll(String queryFile, String resPath) throws IOException {
        benchSearchLatency(queryFile, resPath + "_search");
        benchExtractLatency(resPath + "_extract");
    }

    /**
     * Reads ByteBuffer in from file existing in tfs
     * @param tfs tachyon file system
     * @param file file to read from
     * @param readOps read options
     * @return byte buffer of file
     * @throws IOException
     * @throws TachyonException
     */
    private static ByteBuffer readByteBuf(TachyonFileSystem tfs, TachyonFile file, InStreamOptions readOps)
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
    private static void setupTFS(String masterURI) {
        TachyonURI masterLoc = new TachyonURI(masterURI);
        TachyonConf tachyonConf = ClientContext.getConf();
        tachyonConf.set(Constants.MASTER_HOSTNAME, masterLoc.getHost());
        tachyonConf.set(Constants.MASTER_RPC_PORT, Integer.toString(masterLoc.getPort()));
        ClientContext.reset(tachyonConf);
    }

}
