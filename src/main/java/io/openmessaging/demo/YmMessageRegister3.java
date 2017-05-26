package io.openmessaging.demo;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmMessageRegister3 {
    private int offset = 0;
    private YmBucketCache6[] bucketArray;
    private static YmMessageRegister3 register = new YmMessageRegister3();
    private YmMessageRegister3() {
        bucketArray = new YmBucketCache6[Config.CACHE_NUM];
        for (int i = 0; i < bucketArray.length; i++) {
            bucketArray[i] = new YmBucketCache6();
        }
    }

    public static synchronized YmMessageRegister3 getInstance() {
        return register;
    }

    public synchronized YmBucketCache6 getCache() {
        int r = offset % bucketArray.length;
        offset++;
        return bucketArray[r];
    }
}
