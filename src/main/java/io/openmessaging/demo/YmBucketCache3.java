package io.openmessaging.demo;

import com.sun.org.apache.xerces.internal.xs.LSInputList;
import io.openmessaging.Message;
import io.openmessaging.demo.Util.MessageCounter;
import io.openmessaging.demo.YmSerial.SerialConfig;
import io.openmessaging.demo.YmSerial.YmMessageMeta;
import io.openmessaging.demo.YmWriteModule.YmMessageStore;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by YangMing on 2017/5/9.
 */
public class YmBucketCache3 {
    private List<byte[]> cachedBucket;
    private List<Integer> lengthList;
    private final int MAX_BUCKET_SIZE = Config.MAX_CACHE_SIZE;
    private int currentSize;
    private int realMessageSize;
    private YmMessageStore yms;

    public YmBucketCache3() {
        cachedBucket = new LinkedList<>();
        lengthList = new LinkedList<>();
        yms = YmMessageStore.getInstance();
    }

    public synchronized void addMessage(YmMessageMeta message) {
        cachedBucket.add(message.getMetaData());
        lengthList.add(message.getTotalLength());
        currentSize += SerialConfig.MAX_MESSAGE_SIZE;
        realMessageSize += message.getTotalLength();
        if (isFull()) {
            // call the write module
            System.out.println("bucket full");
            try {
                yms.writeMessage(getCachedBucket(), getLengthList(), realMessageSize);
                MessageCounter.getInstance().countMessage(realMessageSize);
                releaseBucket();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isFull() {
        return currentSize >= MAX_BUCKET_SIZE;
    }

    public List<Integer> getLengthList() {
        return lengthList;
    }

    public List<byte[]> getCachedBucket() {
        return cachedBucket;
    }

    public void releaseBucket() {
        cachedBucket = new LinkedList<>();
        lengthList = new LinkedList<>();
        currentSize = 0;
        realMessageSize = 0;
    }

}
