package io.openmessaging.demo.YmWriteModule;

import io.openmessaging.demo.YmSerial.YmMessageMetaDataParser;
import io.openmessaging.demo.YmSerial.YmMetaDataChunkParser;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

/**
 * Created by YangMing on 2017/5/17.
 */
public class YmMessageReader {
    private long currentPos;
    private int counter = 1;
    private File file;
    private RandomAccessFile raf;
    private MappedByteBuffer mbb;
    private FileChannel fileChannel;
    private final long MAX_BUFFER_SIZE = StoreConfig.MAX_BUFFER_SIZE;
    private static YmMessageReader ymr = new YmMessageReader();
    private int DATA_CHUNK_SIZE = StoreConfig.DATA_CHUNK_SIZE;
    private YmMessageMetaDataParser dataParser;


    public void init() {
        currentPos = 0;
        file = new File(StoreConfig.STORE_PATH + StoreConfig.FILE_NAME + counter);
        if (file.exists()) {
            file.delete();
        }
        try {
            raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();
            mbb = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, MAX_BUFFER_SIZE + DATA_CHUNK_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private YmMessageReader() {
        init();
        dataParser = new YmMessageMetaDataParser();
    }

    public static YmMessageReader getInstance() {
        return ymr;
    }

    public void readNewFile() {
        init();
    }

    public void readData() {
        YmMetaDataChunkParser parser = new YmMetaDataChunkParser();



        while (currentPos < MAX_BUFFER_SIZE + DATA_CHUNK_SIZE) {
            // call the consumer thread
            System.out.println("read data chunk");
            parser.setMetaData(readDataChunk(), 0);
            try {
                parser.readMessage();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public byte[] readDataChunk() {
        byte[] dataChunk = new byte[DATA_CHUNK_SIZE];
        for (int i = 0; i < dataChunk.length; i++) {
            dataChunk[i] = mbb.get();
        }
        currentPos += dataChunk.length;
        return dataChunk;
    }

    public static void main(String[] args) {
        YmMessageReader reader = YmMessageReader.getInstance();
        reader.readData();
    }

    
}
