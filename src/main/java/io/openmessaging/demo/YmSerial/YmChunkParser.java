package io.openmessaging.demo.YmSerial;

import io.openmessaging.demo.DefaultBytesMessage;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by YangMing on 2017/5/22.
 */
public class YmChunkParser {
    private byte[] metaData;
    private int currentOffset;
    private int preOffset;
    private List<DefaultBytesMessage> msgList;
    boolean over;

    public YmChunkParser() {
        msgList = new LinkedList<>();
        currentOffset = 0;
        preOffset = 0;
    }

    public void setMetaData(byte[] metaData) {
        over = false;
        this.metaData = metaData;
    }

    public List<DefaultBytesMessage> getMsgList() {
        return msgList;
    }

    public void readChunk() {
        while (currentOffset < metaData.length) {
            if (over) return;
            try {
                DefaultBytesMessage msg = readMessage();
                if (msg != null) {
                    msgList.add(msg);
                } else break;
            } catch (Exception e) {
                break;
            }
        }
    }

    private DefaultBytesMessage readMessage() throws Exception {
        DefaultBytesMessage msg = new DefaultBytesMessage(null);
        int msg_length = readMessageHeadAndLength(msg);
        if (msg_length == 0) {
            over = true;
            return null;
        }
        while (currentOffset - preOffset < msg_length) {
            readBodyAndKeyValue(msg);
        }
        preOffset = currentOffset;
        return msg;
    }

    private int readMessageHeadAndLength(DefaultBytesMessage msg) throws Exception {
        int signature = readSignature();
        int r;
        if (signature == SerialConfig.SIGNATURE_MESSAGE) {
            r = readLength();
        } else throw new Exception("bad message first signature");
        return r;
    }

    public void readBodyAndKeyValue(DefaultBytesMessage msg) throws Exception{
        int signature = readSignature();
        if (signature == SerialConfig.SIGNATURE_HEADER) {
            readHeaderKeyValue(msg);
        } else if (signature == SerialConfig.SIGNATURE_PROPERTY) {
            // to do ...
            readPropertyKeyValue(msg);
        } else if (signature == SerialConfig.SIGNATURE_BODY) {
            readBody(msg);
        } else if (signature == SerialConfig.SIGNATURE_END) {
            over = true;
        } else {
            throw new Exception("bad signature, offset: " + currentOffset);
        }
    }

    private void readBody(DefaultBytesMessage msg) throws Exception {
        int bodyLength = readLength();
        msg.setBody(getStringBytes(bodyLength));
    }

    private void readHeaderKeyValue(DefaultBytesMessage msg) throws Exception {
        readSignature();
        String key = readString();
        int signature = readSignature();
        if (signature == SerialConfig.SIGNATURE_STRING) msg.putHeaders(key, readString());
        else if (signature == SerialConfig.SIGNATURE_INT) msg.putHeaders(key, readInt());
        else if (signature == SerialConfig.SIGNATURE_LONG) msg.putHeaders(key, readLong());
        else if (signature == SerialConfig.SIGNATURE_DOUBLE) msg.putHeaders(key, readDouble());
        else throw new Exception("bad offset: " + currentOffset);
    }

    private void readPropertyKeyValue(DefaultBytesMessage msg) throws Exception {
        readSignature();
        String key = readString();
        int signature = readSignature();
        if (signature == SerialConfig.SIGNATURE_STRING) msg.putProperties(key, readString());
        else if (signature == SerialConfig.SIGNATURE_INT) msg.putProperties(key, readInt());
        else if (signature == SerialConfig.SIGNATURE_LONG) msg.putProperties(key, readLong());
        else if (signature == SerialConfig.SIGNATURE_DOUBLE) msg.putProperties(key, readDouble());
        else throw new Exception("bad offset: " + currentOffset);
    }


    private int readSignature() {
        return metaData[currentOffset++];
    }

    private int readLength() {
        int r = ((metaData[currentOffset] << 24 & 0xFF000000) |
                (metaData[currentOffset + 1] << 16 & 0x00FF0000) |
                (metaData[currentOffset + 2] << 8 & 0x0000FF00) |
                (metaData[currentOffset + 3] & 0x000000FF));
        currentOffset += 4;
        return r;
    }

    private int readInt() {
        return readLength();
    }

    private long readLong() {
        // to do
        return 1;
    }

    private double readDouble() {
        // to do ...
        return 1.0;
    }

    private byte[] getStringBytes(int stringLength) {
        byte[] r = new byte[stringLength];
        for (int i = currentOffset; i < currentOffset + stringLength; i++) {
            r[i - currentOffset] = metaData[i];
        }
        currentOffset += stringLength;
        return r;
    }

    private String readStringWithHead() throws Exception {
        if (readSignature() == SerialConfig.SIGNATURE_STRING) {
            int length = readLength();
            return new String(getStringBytes(length));
        } else {
            throw new Exception("bad string signature, current offset " + currentOffset);

        }
    }

    private int readIntWithHead() throws Exception {
        if (readSignature() == SerialConfig.SIGNATURE_INT) {
            return readInt();
        } else {
            throw new Exception("bad int signature, current offset " + currentOffset);
        }
    }

    private long readLongWithHead() throws Exception {
        // to do ...
        return 1;
    }

    private double readDoubleWithHead() throws Exception {
        // to do ...
        return 1.0;
    }

    private String readString() {
        return new String(getStringBytes(readLength()));
    }

}
