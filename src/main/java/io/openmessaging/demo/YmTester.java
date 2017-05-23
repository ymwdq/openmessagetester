package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.demo.Util.YmLogUtil;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by YangMing on 2017/5/3.
 */
public class YmTester {
    public static void main(String[] args) {
//        testCache();
//        testNoCache();
//        testBucketCache();
    }

    public static void testCache() {
        YmMessageCache cache = YmMessageCache.getInstance();
        DefaultKeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/ym/download/");
        DefaultProducer producer = new DefaultProducer(properties);
        String[] topics = new String[]{"topic1", "topic1", "topic2", "topic4"};
        String[] queues = new String[]{"queue1", "queue2", "queue3", "queue4"};
        YmLogUtil log = new YmLogUtil();
//        YmSimpleFileUtil file = new YmSimpleFileUtil();
//        file.setArgs("/home/ym/download/", "testCacheWrite.txt", true);
//        file.createFile();
        Message message;
        log.startCount();
        for (int i = 0; i < 1024; i++) {
           for (String topic : topics) {
               message = producer.createBytesMessageToTopic(topic, new byte[182 * 1024]);
               cache.addMessage(message, 128 * 1024);
           }
//           for (String queue : queues) {
//               message = producer.createBytesMessageToQueue(queue, new byte[128 * 1024]);
//               cache.addMessage(message, 128 * 1024);
//           }
           if (cache.isBucketFull()) {
               System.out.println("bucketFull");
//               cache.printStatus();
               LinkedList<Message> messages = cache.getAndReleaseMaxBucket();
               for (Message each : messages) {
//                   file.writeContent(new String(((DefaultBytesMessage)each).getBody()));
               }

           } else if (cache.isTotalFull()) {
               System.out.println("totalFull");
//               cache.printStatus();
               cache.getAndReleaseMaxBucket();
               LinkedList<Message> messages = cache.getAndReleaseMaxBucket();
               for (Message each : messages) {
//                   file.writeContent(new String(((DefaultBytesMessage)each).getBody()));
               }
           }
        }
        ConcurrentHashMap<String, LinkedList<Message>> cachedBuckets  = cache.getCachedBuckets();
        for (String bucket : cachedBuckets.keySet()) {
            LinkedList<Message> messages = cachedBuckets.get(bucket);
            for (Message each : messages) {
//                file.writeContent(new String(((DefaultBytesMessage)each).getBody()));
            }
        }
//        file.flush();
//        file.close();
        log.endCount();
        log.printTime();

    }

    public static void testNoCache () {
        DefaultKeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/ym/download/");
        DefaultProducer producer = new DefaultProducer(properties);
        String[] topics = new String[]{"topic1", "topic1", "topic1", "topic4"};
        String[] queues = new String[]{"queue1", "queue2", "queue3", "queue4"};
        YmLogUtil log = new YmLogUtil();
        YmSimpleFileUtil file = new YmSimpleFileUtil();
        file.setArgs("/home/ym/download/", "testNoCacheWrite.txt", true);
        file.createFile();
        Message message;
        log.startCount();
        for (int i = 0; i < 1024; i++) {
            for (String topic : topics) {
                message = producer.createBytesMessageToTopic(topic, new byte[128 * 1024]);
                file.writeContent(((DefaultBytesMessage)message).getBody());
            }
//           for (String queue : queues) {
//               message = producer.createBytesMessageToQueue(queue, new byte[128 * 1024]);
//               cache.addMessage(message, 128 * 1024);
//           }

        }
        file.flush();
        file.close();
        log.endCount();
        log.printTime();
    }

    public static void testBucketCache() {
        YmBucketCache cache = YmBucketCache.getInstance();
        DefaultKeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/ym/download/");
        DefaultProducer producer = new DefaultProducer(properties);
        String[] topics = new String[]{"topic1", "topic1", "topic2", "topic4"};
        String[] queues = new String[]{"queue1", "queue2", "queue3", "queue4"};
        YmLogUtil log = new YmLogUtil();
        YmSimpleFileUtil file = new YmSimpleFileUtil();
        file.setArgs("/home/ym/download/", "testBucketCache.txt", true);
        file.createFile();
        Message message;
        log.startCount();
        for (int i = 0; i < 10 * 1024; i++) {
            for (String topic : topics) {
                message = producer.createBytesMessageToTopic(topic, new byte[128 * 1024]);
                cache.addMessage(message, 128 * 1024);
            }
//           for (String queue : queues) {
//               message = producer.createBytesMessageToQueue(queue, new byte[128 * 1024]);
//               cache.addMessage(message, 128 * 1024);
//           }
            if (cache.isFull()) {
                System.out.println("bucketFull");
                LinkedList<Message> messages = (LinkedList<Message>) cache.getAndReleaseBucket();
                for (Message each : messages) {
                   file.writeContent(((DefaultBytesMessage)each).getBody());
                }
            }
        }

        LinkedList<Message> cachedBucket = (LinkedList<Message>) cache.getBucket();
        for (Message each : cachedBucket) {
            file.writeContent(((DefaultBytesMessage)each).getBody());
        }

        file.flush();
        file.close();
        log.endCount();
        log.printTime();
    }

    public void test517() {

    }

}
