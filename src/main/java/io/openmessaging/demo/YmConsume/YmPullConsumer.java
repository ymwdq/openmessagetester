package io.openmessaging.demo.YmConsume;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.ClientOMSException;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.mydemo.MyMessageStore;

import java.util.*;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmPullConsumer implements PullConsumer{
    private MyMessageStore messageStore = MyMessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> topics = new HashSet<>();
    private Queue<List<DefaultBytesMessage>> msgQueue;
    private List<DefaultBytesMessage> msgList;
    private boolean isFinish;
    private int offset;

    public YmPullConsumer(KeyValue properties) {
        this.properties = properties;
        isFinish = false;
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {
        if (isFinish) return null;
        // 非阻塞？
        while (true) {
            if (isListConsumeOver()) {
                if (msgQueue.size() > 0) {
                    msgList = msgQueue.poll();
                    offset = 0;
                }
            } else {
                return msgList.get(offset);
            }
        }
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

    public boolean isListConsumeOver() {
        return offset >= msgList.size();
    }


    public void setFinish(boolean isFinish) {
        this.isFinish = isFinish;
    }

}
