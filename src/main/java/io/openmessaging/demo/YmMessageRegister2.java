//package io.openmessaging.demo;
//
///**
// * Created by YangMing on 2017/5/22.
// */
//public class YmMessageRegister2 {
//    private int offset = 0;
//    private YmBucketCache5[] bucketArray;
//    private static YmMessageRegister2 register = new YmMessageRegister2();
//    private YmMessageRegister2() {
//        bucketArray = new YmBucketCache5[Config.CACHE_NUM];
//        for (int i = 0; i < bucketArray.length; i++) {
//            bucketArray[i] = new YmBucketCache5();
//        }
//    }
//
//    public static synchronized YmMessageRegister2 getInstance() {
//        return register;
//    }
//
//    public synchronized YmBucketCache5 getCache() {
//        int r = offset % bucketArray.length;
//        offset++;
//        return bucketArray[r];
//    }
//}
