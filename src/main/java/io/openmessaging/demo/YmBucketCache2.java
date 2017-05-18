package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by YangMing on 2017/5/7.
 */
public class YmBucketCache2 {
    private List<Message> bucket;
    private double MAX_BUCKET_SIZE = Config.MAX_CACHE_SIZE;
    private double currentSize = 0;
    public YmBucketCache2() {
        bucket = new LinkedList<>();
    }

    public synchronized void addMessage(Message message, double bodyLength) {
        bucket.add(message);
        currentSize += bodyLength;
        if (isFull()) {
            // call the write thread
            System.out.println("call the write module");
            getAndReleaseBucket();
        }
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
