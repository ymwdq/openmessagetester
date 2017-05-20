package io.openmessaging.demo.YmWriteModule;

import io.openmessaging.demo.YmSerial.SerialConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;

/**
 * Created by YangMing on 2017/5/20.
 */
public class YmMessageStore2 {
    private File file;
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    private final long MAX_BUFFER_SIZE = StoreConfig.MAX_BUFFER_SIZE;
    private MappedByteBuffer mbb;
    private long currentPos;
    private int counter = 1;
    private static YmMessageStore2 yms = new YmMessageStore2();
    private YmMessageStore2() {
        init();
    }

    public synchronized static YmMessageStore2 getInstance() {
        return yms;
    }

    private void init() {
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

    public synchronized void writeMessage  (HashMap<String, List<byte[]>> bytes, int totalLength) throws IOException {
        if ((totalLength + currentPos) >= MAX_BUFFER_SIZE) {
            writeEndBytes();
            fileChannel.close();
            System.out.println("write buffer full" + counter);
            counter += 1;
            init();
        }
        for (String key : bytes.keySet()) {
            List<byte[]> eachTopicOrQueue = bytes.get(key);
            for (byte[] eachMessage : eachTopicOrQueue) {
                mbb.put(eachMessage, 0, eachMessage.length);
            }
        }
        currentPos += totalLength;
    }

    public void writeEndBytes() {
        mbb.put(new byte[]{(byte) SerialConfig.SIGNATURE_END});
    }

}
