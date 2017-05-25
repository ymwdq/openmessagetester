package io.openmessaging.mydemo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.ClientOMSException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Copyright (c) 2017 XiaoMi Inc. All Rights Reserved.
 * Authors: liujinhong <liujinhong@xiaomi.com>.
 * Created on 2017/5/9.
 */
public class MyDefaultPullConsumer implements PullConsumer {
    private MyMessageStore messageStore = MyMessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> topics = new HashSet<>();

    public MyDefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {
        return messageStore.pollMessage(queue);
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        this.topics.addAll(topics);

        messageStore.attachQueue(queue, this.topics);
    }
}
