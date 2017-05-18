package io.openmessaging.demo;

/**
 * Created by YangMing on 2017/5/9.
 */
public class YmMessageCollector2 {
    private int offset = 0;
    private YmBucketCache3[] bucketArray;
    private static YmMessageCollector2 collector = new YmMessageCollector2();
    private YmMessageCollector2() {
        bucketArray = new YmBucketCache3[Config.CACHE_NUM];
        for (int i = 0; i < bucketArray.length; i++) {
            bucketArray[i] = new YmBucketCache3();
        }
    }

    public static synchronized YmMessageCollector2 getInstance() {
        return collector;
    }

    public synchronized YmBucketCache3 getCache() {
        int r = offset % bucketArray.length;
        offset++;
        return bucketArray[r];
    }
}
