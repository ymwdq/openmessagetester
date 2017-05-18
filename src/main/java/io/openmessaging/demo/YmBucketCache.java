package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by YangMing on 2017/5/4.
 */
public class YmBucketCache {
    private List<Message> bucket;
    private double MAX_BUCKET_SIZE = 256 * 1024 * 1024;
    private double currentSize = 0;
    private static YmBucketCache cache = new YmBucketCache();
    private YmBucketCache() {
        bucket = new LinkedList<>();
    }

    public static YmBucketCache getInstance() {
        return cache;
    }

    public synchronized void addMessage(Message message, double bodyLength) {
        bucket.add(message);
        currentSize += bodyLength;
    }

    public boolean isFull() {
        return currentSize >= MAX_BUCKET_SIZE;
    }

    public List<Message> getAndReleaseBucket() {
        List<Message> r = bucket;
        bucket = new LinkedList<>();
        currentSize = 0;
        return r;
    }

    public List<Message> getBucket() {
        return this.bucket;
    }

    public double getCurrentSize() {
        return this.currentSize;
    }

}
