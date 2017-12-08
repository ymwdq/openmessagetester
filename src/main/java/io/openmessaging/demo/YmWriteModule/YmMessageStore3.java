package io.openmessaging.demo.YmWriteModule;

import io.openmessaging.Message;
import io.openmessaging.demo.YmSerial.SerialConfig;
import io.openmessaging.demo.YmSerial.YmMessageMeta;
import io.openmessaging.demo.YmSerial.YmMessageMeta2;
import io.openmessaging.demo.YmSerial.YmMessageMeta3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;

/**
 * Created by YangMing on 2017/5/22.
 */
public class YmMessageStore3 {
    private File file;
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    private final long MAX_BUFFER_SIZE = StoreConfig.MAX_BUFFER_SIZE;
    private MappedByteBuffer mbb;
    private long currentPos;
    private int counter = 1;
    private String storePath;
    private boolean hasInit = false;
    private static YmMessageStore3 yms = new YmMessageStore3();
    private YmMessageStore3() {
        init();
    }

    public synchronized static YmMessageStore3 getInstance() {
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

    public synchronized void writeMessage(HashMap<String, List<YmMessageMeta3>> msgTable, int totalLength) throws IOException {
        if ((totalLength + currentPos) >= MAX_BUFFER_SIZE) {
            writeEndBytes();
            fileChannel.close();
            System.out.println("write to disk " + counter);
            counter += 1;
            init();
        }
        for (String key : msgTable.keySet()) {
            List<YmMessageMeta3> eachTopicOrQueue = msgTable.get(key);
            for (YmMessageMeta3 eachMessage : eachTopicOrQueue) {
                mbb.put(eachMessage.getRealMetaData(), 0, eachMessage.getMetaDataLength());
                mbb.put(eachMessage.getBody(), 0, eachMessage.getBody().length);
            }
        }
            currentPos += totalLength;
    }

    public void writeEndBytes() {
        mbb.put(new byte[]{(byte) SerialConfig.SIGNATURE_END});
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
        init();
    }

    public boolean hasInit() {
        return hasInit;
    }
}
