package io.openmessaging.demo;

import io.openmessaging.Producer;

/**
 * Created by YangMing on 2017/5/4.
 */
public class YmMessageCollector {
    private int offset = 0;
    private YmBucketCache2[] bucketArray;
    private static YmMessageCollector collector = new YmMessageCollector();
    private YmMessageCollector() {
        bucketArray = new YmBucketCache2[Config.CACHE_NUM];
        for (int i = 0; i < bucketArray.length; i++) {
            bucketArray[i] = new YmBucketCache2();
        }
    }

    public static synchronized YmMessageCollector getInstance() {
        return collector;
    }

    public synchronized YmBucketCache2 getCache() {
        int r = offset % bucketArray.length;
        offset++;
        return bucketArray[r];
    }

}
