package io.openmessaging.demo;

/**
 * Created by YangMing on 2017/5/20.
 */
public class YmMessageRegister {
    private int offset = 0;
    private YmBucketCache4[] bucketArray;
    private static YmMessageRegister register = new YmMessageRegister();
    private YmMessageRegister() {
        bucketArray = new YmBucketCache4[Config.CACHE_NUM];
        for (int i = 0; i < bucketArray.length; i++) {
            bucketArray[i] = new YmBucketCache4();
        }
    }

    public static synchronized YmMessageRegister getInstance() {
        return register;
    }

    public synchronized YmBucketCache4 getCache() {
        int r = offset % bucketArray.length;
        offset++;
        return bucketArray[r];
    }
}
