package io.openmessaging.demo;

import io.openmessaging.demo.Util.MessageCounter;
import io.openmessaging.demo.YmSerial.YmMessageMeta;
import io.openmessaging.demo.YmWriteModule.YmMessageStore2;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by YangMing on 2017/5/20.
 */
public class YmBucketCache4 {
    private HashMap<String, List<byte[]>> cachedBucket;
    private final int MAX_BUCKET_SIZE = Config.MAX_CACHE_SIZE;
    private int currentSize;
    private YmMessageStore2 yms;

    public YmBucketCache4() {
        cachedBucket = new HashMap<>(Config.MESSAGE_CACHE_HASH_TABLE_SIZE);
        yms = YmMessageStore2.getInstance();
    }

    public synchronized void addMessage(YmMessageMeta message, String queueOrTopic) {
        currentSize += message.getTotalLength();
        if (cachedBucket.containsKey(queueOrTopic)) {
            cachedBucket.get(queueOrTopic).add(message.getRealMetaData());
        } else {
            List<byte[]> tmp = new LinkedList<>();
            tmp.add(message.getRealMetaData());
            cachedBucket.put(queueOrTopic, tmp);
        }
        if (isFull()) {
            // call the write module
            try {
                System.out.println("bucket full");
                yms.writeMessage(getCachedBucket(), currentSize);
//                MessageCounter.getInstance().countMessage(currentSize);
                releaseBucket();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isFull() {
        return currentSize >= MAX_BUCKET_SIZE;
    }


    public HashMap<String, List<byte[]>> getCachedBucket() {
        return cachedBucket;
    }

    public void releaseBucket() {
        cachedBucket = new HashMap<>(Config.MESSAGE_CACHE_HASH_TABLE_SIZE);
        currentSize = 0;
    }
}
