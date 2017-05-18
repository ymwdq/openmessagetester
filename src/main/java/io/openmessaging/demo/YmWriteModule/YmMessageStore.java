package io.openmessaging.demo.YmWriteModule;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by YangMing on 2017/5/16.
 */
public class YmMessageStore {
    private File file;
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    private final long MAX_BUFFER_SIZE = StoreConfig.MAX_BUFFER_SIZE;
    private final int DATA_CHUNK_SIZE = StoreConfig.DATA_CHUNK_SIZE;
    private MappedByteBuffer mbb;
    private long currentPos;
    private int chunkPos;
    private int counter = 1;
    private static YmMessageStore yms = new YmMessageStore();
    private YmMessageStore() {
        init();
    }

    public synchronized static YmMessageStore getInstance() {
        return yms;
    }

    public void init() {
        currentPos = 0;
        file = new File(StoreConfig.STORE_PATH + StoreConfig.FILE_NAME + counter);
        if (file.exists()) {
            file.delete();
        }
        try {
            raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();
            mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_BUFFER_SIZE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

//    public void alignDataChunk() {
//        System.out.println("align data chunk");
//        mbb.put(new byte[DATA_CHUNK_SIZE - chunkPos]);
//        currentPos += (DATA_CHUNK_SIZE - chunkPos);
//        chunkPos = 0;
//    }

//    public synchronized void writeMessage  (List<byte[]> bytes, List<Integer> lengthList, int totalLength) throws IOException {
//        if ((totalLength + currentPos) >= MAX_BUFFER_SIZE) {
//            System.out.println("write buffer full" + counter);
//            fileChannel.close();
//            counter += 1;
//            init();
//        }
//        Iterator iter = lengthList.iterator();
//        for (byte[] each : bytes) {
//            int messageSize = (int)iter.next();
//            if ((messageSize + chunkPos) >= (DATA_CHUNK_SIZE)) {
//                alignDataChunk();
//            }
//            mbb.put(each, 0, messageSize);
//            currentPos += messageSize;
//            chunkPos += messageSize;
//        }
//    }

    public synchronized void writeMessage  (List<byte[]> bytes, List<Integer> lengthList, int totalLength) throws IOException {
        if ((totalLength + currentPos) >= MAX_BUFFER_SIZE) {
            System.out.println("write buffer full" + counter);
            fileChannel.close();
            counter += 1;
            init();
        }
        Iterator iter = lengthList.iterator();
        for (byte[] each : bytes) {
            int messageSize = (int)iter.next();
            mbb.put(each, 0, messageSize);
        }
        currentPos += totalLength;
    }

}
