package io.openmessaging.mydemo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.PullConsumer;

import java.util.Collections;
import java.util.List;

public class ConsumerTester {
    static class ConsumerTask extends Thread {
        String queue;
        List<String> topics;
        PullConsumer consumer;
        public ConsumerTask(String queue, List<String> topics) {
            this.queue = queue;
            this.topics = topics;
            init();
        }

        public void init() {
            //init consumer
            try {
                Class kvClass = Class.forName("io.openmessaging.demo.DefaultKeyValue");
                KeyValue keyValue = (KeyValue) kvClass.newInstance();
                keyValue.put("STORE_PATH", Constants.STORE_PATH);
                Class consumerClass = Class.forName("io.openmessaging.mydemo.MyDefaultPullConsumer");
                consumer = (PullConsumer) consumerClass.getConstructor(new Class[]{KeyValue.class}).newInstance(new Object[]{keyValue});
                if (consumer == null) {
                    throw new InstantiationException("Init Producer Failed");
                }
                consumer.attachQueue(queue, topics);
            } catch (Exception e) {
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    //Thread.sleep(1);
                    BytesMessage message = (BytesMessage) consumer.poll();
                    //System.out.println("get message");

                } catch (Exception e) {
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Thread[] ts = new Thread[Constants.CON_NUM];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new ConsumerTask(Constants.QUEUE_PRE + i,
                Collections.singletonList(Constants.TOPIC_PRE + i));
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
        long end = System.currentTimeMillis();

        System.out.println(end - start);
    }
}
