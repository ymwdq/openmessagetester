package io.openmessaging.mydemo;

import io.openmessaging.Message;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.YmSerial.YmChunkParser;
import io.openmessaging.demo.YmWriteModule.YmMessageReader2;

import java.util.*;

/**
 * Copyright (c) 2017 XiaoMi Inc. All Rights Reserved.
 * Authors: liujinhong <liujinhong@xiaomi.com>.
 * Created on 2017/5/9.
 */
public class MyMessageStore {
    //MyMessageStore单例
    private static final MyMessageStore INSTANCE = new MyMessageStore();
    //获取单例对象
    public static MyMessageStore getInstance() {return INSTANCE;}
    //存储topic和queue的绑定关系，来指明一个topic要发送到哪些queue里面
    private Map<String, Set<String>> consumerAttachment;
    //分队列存储消息
    private Map<String, Queue<Message>> messageQueues;
    //判断数据读取线程是否启动
    public volatile boolean start;

    //message批量缓存
    private volatile List<DefaultBytesMessage> messagePool;
    //message批量缓存，用于缓存切换
    private volatile List<DefaultBytesMessage> messagePoolTemp;

    private Set<String> queuesAndTopics;

    private YmMessageReader2 ymMessageReader;
    private YmChunkParser ymChunkParser;

    private volatile boolean readOver = false;

    private MyMessageStore() {
        start = false;
        messagePool= null;
        messagePoolTemp = null;
        consumerAttachment = new HashMap<>();
        messageQueues = new HashMap<>();
        queuesAndTopics = new HashSet<>();
        ymMessageReader = YmMessageReader2.getInstance();
        ymChunkParser = new YmChunkParser();

        //启动读数据线程
        ReadMessageThread reader = new ReadMessageThread();
        reader.start();

        //启动数据消费线程
        DistributeMessageThread writer = new DistributeMessageThread();
        writer.start();
    }

    class ReadMessageThread extends Thread {
        @Override
        public void run() {
            System.out.println("ReadMessageThread");
            while (true) {
                if (messagePoolTemp == null) {
                    ymChunkParser.setMetaData(ymMessageReader.readDataChunk());
                    try {
                        ymChunkParser.readChunk();
                        messagePoolTemp = ymChunkParser.getMsgList();
                        if (messagePool == null) {
                            messagePool = messagePoolTemp;
                            messagePoolTemp = null;
                        }
                    } catch (Exception e) {
                        System.out.println(e);
                        readOver = true;
                        break;
                    }
                }
            }
        }
    }

    class DistributeMessageThread extends Thread {
        @Override
        public void run() {
            //读线程等待所有的消费者注册完毕
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("DistributeMessageThread");

            //开始消费数据
            while (!readOver || messagePool != null) {
                if (messagePool != null) {
                    for (int i = 0; i < messagePool.size(); i++) {
                        DefaultBytesMessage message = messagePool.get(i);

                        String topic = message.headers().getString("Topic");
                        if (topic == null) topic = message.headers().getString("Queue");

                        System.out.println(topic);

                        if (consumerAttachment.containsKey(topic)) {
                            Set<String> queues = consumerAttachment.get(topic);
                            for (String queue : queues) {
                                offerMessage(queue, message);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 建立topic到queue的映射关系，在分发消息的时候来确定某个topic的消息要发送到哪些queue里
     * @param queue
     * @param topics
     */
    public synchronized void attachQueue(String queue, Set<String> topics) {
        topics.add(queue);
        for (String topic : topics) {
            if (!consumerAttachment.containsKey(topic)) {
                consumerAttachment.put(topic, new HashSet<>());
            }
            consumerAttachment.get(topic).add(queue);
        }
        queuesAndTopics.addAll(topics);

        Queue<Message> messageQueue = new LinkedList<>();
        messageQueues.put(queue, messageQueue);
    }

    public Message pollMessage(String queue) throws NullPointerException{
        Queue<Message> messageQueue = messageQueues.get(queue);
        if (messageQueue == null) return null;
        synchronized (messageQueue) {
            return messageQueue.poll();
        }
    }

    public void offerMessage(String queue, Message message) {
        Queue<Message> messageQueue = messageQueues.get(queue);
        synchronized (messageQueue) {
            messageQueue.offer(message);
        }
    }
}