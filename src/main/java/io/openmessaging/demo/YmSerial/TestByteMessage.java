package io.openmessaging.demo.YmSerial;

import io.openmessaging.demo.DefaultBytesMessage;

/**
 * Created by YangMing on 2017/5/9.
 */
public class TestByteMessage {
    public static void main(String[] args) throws Exception {
        YmMessageMeta metaMessage = new YmMessageMeta(new byte[] {9,8});
        metaMessage.putHeaders("666", 3);
        metaMessage.putHeaders("TOPIC1", "1111");
        metaMessage.putHeaders("TOPIC1", "2222");
        byte[] metaData = metaMessage.getMetaData();

        YmMessageMetaDataParser metaParser = new YmMessageMetaDataParser();
        metaParser.setMetaData(metaData);
        metaParser.readMessage();
        DefaultBytesMessage parsedMsg = (DefaultBytesMessage)metaParser.getMsg();
        System.out.println(parsedMsg);
        System.out.println("over");
    }
}
