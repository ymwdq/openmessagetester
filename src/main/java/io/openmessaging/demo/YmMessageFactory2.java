package io.openmessaging.demo;

import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.YmSerial.YmMessageMeta;
import io.openmessaging.demo.YmSerial.YmMessageMeta2;

/**
 * Created by YangMing on 2017/5/22.
 */
public class YmMessageFactory2 implements MessageFactory{
    @Override public YmMessageMeta2 createBytesMessageToTopic(String topic, byte[] body) {
        YmMessageMeta2 metaMsg = new YmMessageMeta2(body);
        metaMsg.setBody(body);
        metaMsg.putHeaders(MessageHeader.TOPIC, topic);
        return metaMsg;
    }

    @Override public YmMessageMeta2 createBytesMessageToQueue(String queue, byte[] body) {
        YmMessageMeta2 metaMsg = new YmMessageMeta2(body);
        metaMsg.setBody(body);
        metaMsg.putHeaders(MessageHeader.QUEUE, queue);
        return metaMsg;
    }

}
