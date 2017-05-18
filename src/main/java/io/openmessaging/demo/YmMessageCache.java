package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by YangMing on 2017/5/3.
 */
public class YmMessageCache {
    // max block size is 128MB
    private final double MAX_BUCKET_SIZE = 1024 * 8 * 1024 ;
    // total max cache size is 2048MB
    private final double MAX_CACHE_SIZE = 128;
    private int current_size = 0;
    private String current_max_bucket = null;
    private int current_max_bucket_size = 0;
    // capacity is 2048 / 128 * 4, key is the name of queue or topic
    private ConcurrentHashMap<String, LinkedList<Message>> cachedBuckets = new ConcurrentHashMap<>(64);
    private ConcurrentHashMap<String, Integer> cachedBucketsSize = new ConcurrentHashMap<>(64);
    private static YmMessageCache cache = new YmMessageCache();

    private YmMessageCache() {

    }


    // it may not be synchronized
    public static YmMessageCache getInstance() {
        return cache;
    }

    public synchronized void addMessage(Message message, int bodySize) {
        String bucketName = message.headers().getString(MessageHeader.TOPIC) == null ?  message.headers().getString(MessageHeader.QUEUE) :  message.headers().getString(MessageHeader.TOPIC);

        // first add message else not first
        if (current_max_bucket == null) {
            current_max_bucket = bucketName;
            current_max_bucket_size = bodySize;
            LinkedList<Message> messages = new LinkedList<>();
            messages.add(message);
            cachedBuckets.put(bucketName, messages);
            cachedBucketsSize.put(bucketName, bodySize);
        } else {
            if (cachedBuckets.containsKey(bucketName)) {
                cachedBuckets.get(bucketName).add(message);
                cachedBucketsSize.put(bucketName, cachedBucketsSize.get(bucketName) + bodySize);

            } else {
                LinkedList<Message> messages = new LinkedList<>();
                messages.add(message);
                cachedBuckets.put(bucketName, messages);
                cachedBucketsSize.put(bucketName, bodySize);
            }
            int tmpSize = cachedBucketsSize.get(bucketName);
            if (tmpSize > current_max_bucket_size) {
                current_max_bucket_size = tmpSize;
                current_max_bucket = bucketName;
            }
        }
        current_size += bodySize;
    }

    public boolean isTotalFull () {
        return this.current_size >= MAX_CACHE_SIZE;
    }

    public boolean isBucketFull () {
        System.out.println(cachedBuckets);
        System.out.println(cachedBucketsSize);
        return this.current_max_bucket_size >= MAX_BUCKET_SIZE;
    }

    public LinkedList<Message> getAndReleaseMaxBucket() {
        LinkedList<Message> messages = cachedBuckets.get(current_max_bucket);
        cachedBuckets.put(current_max_bucket, new LinkedList<>());
        cachedBucketsSize.put(current_max_bucket, 0);
        current_size -= current_max_bucket_size;
        int tmpMax = 0;
        for (String key : cachedBucketsSize.keySet()) {
            if (cachedBucketsSize.get(key) >= tmpMax) {
                tmpMax = cachedBucketsSize.get(key);
                current_max_bucket = key;
            }
        }
        current_max_bucket_size = tmpMax;
        return messages;
    }

    public void printStatus () {
        System.out.println(current_max_bucket);
        System.out.println(current_max_bucket_size);
        System.out.println(current_size);
    }

    public ConcurrentHashMap<String, LinkedList<Message>> getCachedBuckets () {
        return this.cachedBuckets;
    }
}
