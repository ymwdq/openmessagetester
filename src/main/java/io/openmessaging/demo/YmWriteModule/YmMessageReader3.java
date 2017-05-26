package io.openmessaging.demo.YmWriteModule;

import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.Util.YmLogUtil;
import io.openmessaging.demo.YmSerial.YmChunkParser;
import io.openmessaging.demo.YmSerial.YmChunkParser2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmMessageReader3 {
    private int counter = 1;
    private File file;
    private RandomAccessFile raf;
    private MappedByteBuffer mbb;
    private FileChannel fileChannel;

    private final long MAX_BUFFER_SIZE = StoreConfig.MAX_BUFFER_SIZE;
    private static YmMessageReader3 ymr = new YmMessageReader3();
    private YmChunkParser2 chunkParser = new YmChunkParser2();

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

    private YmMessageReader3() {
        init();
        chunkParser = new YmChunkParser2();
    }

    public static YmMessageReader3 getInstance() {
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
        HashMap<String, List<DefaultBytesMessage>> table = chunkParser.getTable();
        System.out.println("read over");
    }

    public static void main(String[] args) {
        YmLogUtil timer = new YmLogUtil();
        timer.startCount();
        YmMessageReader3 reader = YmMessageReader3.getInstance();
        reader.readData();
        timer.endCount();
        timer.printTime();
    }

}
