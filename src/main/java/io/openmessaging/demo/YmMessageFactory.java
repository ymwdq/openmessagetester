package io.openmessaging.demo;

import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.YmSerial.YmMessageMeta;

/**
 * Created by YangMing on 2017/5/9.
 */
public class YmMessageFactory implements MessageFactory{
    @Override public YmMessageMeta createBytesMessageToTopic(String topic, byte[] body) {
        YmMessageMeta metaMsg = new YmMessageMeta(body);
        metaMsg.setBody(body);
        metaMsg.putHeaders(MessageHeader.TOPIC, topic);
        return metaMsg;
    }

    @Override public YmMessageMeta createBytesMessageToQueue(String queue, byte[] body) {
        YmMessageMeta metaMsg = new YmMessageMeta(body);
        metaMsg.setBody(body);
        metaMsg.putHeaders(MessageHeader.QUEUE, queue);
        return metaMsg;
    }


}
