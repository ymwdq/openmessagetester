package io.openmessaging.demo;

import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.YmSerial.YmMessageMeta3;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmMessageFactory3 implements MessageFactory{
    @Override public YmMessageMeta3 createBytesMessageToTopic(String topic, byte[] body) {
        YmMessageMeta3 metaMsg = new YmMessageMeta3(body);
        metaMsg.setBody(body);
        metaMsg.putHeaders(MessageHeader.TOPIC, topic);
        metaMsg.refreshBodyByte();
        metaMsg.refreshLengthByte();
        return metaMsg;
    }

    @Override public YmMessageMeta3 createBytesMessageToQueue(String queue, byte[] body) {
        YmMessageMeta3 metaMsg = new YmMessageMeta3(body);
        metaMsg.setBody(body);
        metaMsg.putHeaders(MessageHeader.QUEUE, queue);
        metaMsg.refreshBodyByte();
        metaMsg.refreshLengthByte();
        return metaMsg;
    }
}
