package io.openmessaging.demo.YmWriteModule;

import io.openmessaging.demo.Util.YmLogUtil;
import io.openmessaging.demo.YmSerial.YmChunkParser;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by YangMing on 2017/5/22.
 */
public class YmMessageReader2 {
    private int counter = 1;
    private File file;
    private RandomAccessFile raf;
    private MappedByteBuffer mbb;
    private FileChannel fileChannel;
    private final long MAX_BUFFER_SIZE = StoreConfig.MAX_BUFFER_SIZE;
    private static YmMessageReader2 ymr = new YmMessageReader2();
    private YmChunkParser chunkParser = new YmChunkParser();

    public void init() {
        file = new File(StoreConfig.STORE_PATH + StoreConfig.FILE_NAME + counter);
        try {
            raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();
            mbb = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, MAX_BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private YmMessageReader2() {
        init();
        chunkParser = new YmChunkParser();
    }

    public static YmMessageReader2 getInstance() {
        return ymr;
    }

    public void readNewFile() {
        init();
    }


    public byte[] readDataChunk() {
        byte[] dataChunk = new byte[(int)MAX_BUFFER_SIZE];
        mbb.get(dataChunk);
        return dataChunk;
    }

    public void readData() {
        byte[] dataChunk = new byte[(int)MAX_BUFFER_SIZE];
        mbb.get(dataChunk);
        chunkParser.setMetaData(dataChunk);
        chunkParser.readChunk();
        System.out.println(chunkParser.getMsgList().size());
    }

    public static void main(String[] args) {
        YmLogUtil timer = new YmLogUtil();
        timer.startCount();
        YmMessageReader2 reader = YmMessageReader2.getInstance();
        reader.readData();
        timer.endCount();
        timer.printTime();
    }

}
