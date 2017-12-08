package io.openmessaging.demo.YmSerial;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultBytesMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmChunkParser2 {
    private byte[] metaData;
    private int currentOffset;
    private int preOffset;
    private HashMap<String, List<DefaultBytesMessage>> table;
    boolean over;

    public YmChunkParser2() {
        table = new HashMap<>(SerialConfig.CHUNK_TABLE_INIT_SIZE);
        currentOffset = 0;
        preOffset = 0;
    }

    public void setMetaData(byte[] metaData) {
        over = false;
        this.metaData = metaData;
    }

    public HashMap<String, List<DefaultBytesMessage>> getTable() {
        return table;
    }

    public void readChunk() {
        while (currentOffset < metaData.length) {
            if (over) return;
            try {
                DefaultBytesMessage msg = readMessage();
                if (msg != null) {
                    addTable(msg);
                } else break;
            } catch (Exception e) {
                break;
            }
        }
    }

    public void addTable(DefaultBytesMessage msg) {
        String topicOrQueue = msg.headers().getString(MessageHeader.QUEUE);
        if (topicOrQueue == null) topicOrQueue = msg.headers().getString(MessageHeader.TOPIC);
        if (table.containsKey(topicOrQueue)) {
            table.get(topicOrQueue).add(msg);
        } else {
//            List<DefaultBytesMessage> list = new LinkedList<>();
            List<DefaultBytesMessage> list = new ArrayList<>();
            list.add(msg);
            table.put(topicOrQueue, list);
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

    private byte[] getStringBytes(int stringLength) {
        byte[] r = new byte[stringLength];
        for (int i = currentOffset; i < currentOffset + stringLength; i++) {
            r[i - currentOffset] = metaData[i];
        }
        currentOffset += stringLength;
        return r;
    }

    private String readString() {
        return new String(getStringBytes(readLength()));
    }

    private double readDoubleLength(){
        long accum = 0;
        accum = metaData[currentOffset] & 0xFF;
        accum |= (long) (metaData[currentOffset + 1] & 0xFF) << 8;
        accum |= (long) (metaData[currentOffset + 2] & 0xFF) << 16;
        accum |= (long) (metaData[currentOffset + 3] & 0xFF) << 24;
        accum |= (long) (metaData[currentOffset + 4] & 0xFF) << 32;
        accum |= (long) (metaData[currentOffset + 5] & 0xFF) << 40;
        accum |= (long) (metaData[currentOffset + 6] & 0xFF) << 48;
        accum |= (long) (metaData[currentOffset + 7] & 0xFF) << 56;
        currentOffset += 8;
        return Double.longBitsToDouble(accum);
    }

    public long readLongLength(){
        long accum = 0;
        accum = metaData[currentOffset] & 0xFF;
        accum |= (long) (metaData[currentOffset + 1] & 0xFF) << 8;
        accum |= (long) (metaData[currentOffset + 2] & 0xFF) << 16;
        accum |= (long) (metaData[currentOffset + 3] & 0xFF) << 24;
        accum |= (long) (metaData[currentOffset + 4] & 0xFF) << 32;
        accum |= (long) (metaData[currentOffset + 5] & 0xFF) << 40;
        accum |= (long) (metaData[currentOffset + 6] & 0xFF) << 48;
        accum |= (long) (metaData[currentOffset + 7] & 0xFF) << 56;
        currentOffset += 8;
        return accum;
    }

    private long readLong() {
        return readLongLength();
    }

    private double readDouble() {
        return readDoubleLength();
    }


}
