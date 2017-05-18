package io.openmessaging.demo;

import io.openmessaging.demo.YmSerial.YmMessageMeta;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by YangMing on 2017/5/10.
 */
public class YmMessageCache2 {
    private int MAX_SIZE = Config.MAX_CACHE_SIZE;
    private int currentSize = 0;
    private HashMap<String, List<byte[]>> cachedMessage;


    public YmMessageCache2() {
        cachedMessage = new HashMap<>(Config.MESSAGE_CACHE_TABLE_INIT_SIZE);
    }
    public synchronized void addMessage (YmMessageMeta metaMessage, String topicOrQueue) {
         if (!cachedMessage.containsKey(topicOrQueue)) {
             List<byte[]> tmp = new LinkedList<>();
             tmp.add(metaMessage.getMetaData());
             cachedMessage.put(topicOrQueue, tmp);
         } else {
             (cachedMessage.get(topicOrQueue)).add(metaMessage.getMetaData());
         }
         currentSize += metaMessage.getTotalLength();
         if (isFull()) {
             // call the write module, when calling block this cache
             getAndReleaseCache();
         }
    }

    public synchronized boolean isFull() {
        return currentSize >= MAX_SIZE;
    }

    public synchronized HashMap<String, List<byte[]>> getAndReleaseCache() {
        HashMap<String, List<byte[]>> r = cachedMessage;
        cachedMessage = new HashMap<>(Config.MESSAGE_CACHE_TABLE_INIT_SIZE);
        return r;
    }
}
