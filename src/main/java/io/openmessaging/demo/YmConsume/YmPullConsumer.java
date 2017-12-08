package io.openmessaging.demo.YmConsume;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.ClientOMSException;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.YmSerial.SerialConfig;
import io.openmessaging.demo.YmWriteModule.StoreConfig;
import io.openmessaging.demo.mydemo.MyMessageStore;

import java.util.*;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmPullConsumer implements PullConsumer{
    private KeyValue properties;
    private String queue;
    private Set<String> topics = new HashSet<>();
    private List<DefaultBytesMessage> msgList;
    private boolean isFinish;
    private int offset;
    private String currentTopicOrQueue;
    private YmMessageDistributor distributor;

    public YmPullConsumer(KeyValue properties) {
        this.properties = properties;
        StoreConfig.STORE_PATH = properties.getString("STORE_PATH");
        System.out.println("store path" + properties.getString("STORE_PATH"));
        distributor = YmMessageDistributor.getInstance();
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {
        if (isFinish) return null;
        else if (msgList == null) {
            distributor.query(this);
            return this.poll();
        }
        else if (isListConsumeOver()) {
            System.out.println("consume over");
            msgList = null;
            initOffset();
            distributor.consumeOver(this);
            distributor.query(this);
            return this.poll();
        } else {
            return msgList.get(offset++);
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
        distributor.submitConsumer(this, queueName, topics);

    }

    public boolean isListConsumeOver() {
        if (offset >= msgList.size()) {
            System.out.println("offset >= list size");
            return true;
        } else {
            return false;
        }
    }

    public void setFinish(boolean isFinish) {
        this.isFinish = isFinish;
    }

    public void initOffset() {
        System.out.println("init offset");
        offset = 0;
    }

    public void setMsgList(List<DefaultBytesMessage> msgList, String topicOrQueue) {
        this.msgList = msgList;
        this.currentTopicOrQueue = topicOrQueue;
    }

    public List<DefaultBytesMessage> getMsgList() {
        return msgList;
    }

    public String getCurrentTopicOrQueue() {
        return currentTopicOrQueue;
    }

    public boolean isEmpty() {
        if (msgList == null) {
            System.out.println(Thread.currentThread().getName() + "msg list is null");
        }
        return msgList == null;
    }
}
