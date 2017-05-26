package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.demo.YmSerial.YmMessageMeta2;
import io.openmessaging.demo.YmSerial.YmMessageMeta3;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmProducer6 implements Producer {
    private YmMessageFactory3 messageFactory = new YmMessageFactory3();
    private YmBucketCache6 cache;
    private KeyValue properties;
    private String topicOrQueue;

    public YmProducer6(KeyValue properties) {
        this.properties = properties;
        // get the BucketCache
        this.cache = YmMessageRegister3.getInstance().getCache();
    }

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        topicOrQueue = topic;
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        topicOrQueue = queue;
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public void send(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");
//        String topic = message.headers().getString(MessageHeader.TOPIC);
//        String queue = message.headers().getString(MessageHeader.QUEUE);
//        if ((topic == null && queue == null) || (topic != null && queue != null)) {
//            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
//        }
        cache.addMessage((YmMessageMeta3)message, topicOrQueue);
    }

    @Override
    public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    public void flush() {
        // to do ...
        this.cache.flush();
    }

}
