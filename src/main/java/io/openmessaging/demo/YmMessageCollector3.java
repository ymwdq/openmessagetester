package io.openmessaging.demo;

/**
 * Created by YangMing on 2017/5/10.
 */
public class YmMessageCollector3 {
    private int offset = 0;
    private YmBucketCache3[] bucketArray;
    private static YmMessageCollector3 collector = new YmMessageCollector3();
    private YmMessageCollector3() {
        bucketArray = new YmBucketCache3[Config.CACHE_NUM];
        for (int i = 0; i < bucketArray.length; i++) {
            bucketArray[i] = new YmBucketCache3();
        }
    }

    public static synchronized YmMessageCollector3 getInstance() {
        return collector;
    }

    public synchronized YmBucketCache3 getCache() {
        int r = offset % bucketArray.length;
        offset++;
        return bucketArray[r];
    }

}


