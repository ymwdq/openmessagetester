package io.openmessaging.demo.YmSerial;

import io.openmessaging.Message;
import io.openmessaging.demo.DefaultBytesMessage;

/**
 * Created by YangMing on 2017/5/8.
 */
public class YmMessageMetaDataParser {
    private DefaultBytesMessage msg;
    private byte[] metaData;
    private int currentOffset;
    private int totalLength;

    public YmMessageMetaDataParser() {
        this.currentOffset = 0;
    }

    public void setMetaData(byte[] metaData) {
        this.metaData = metaData;
        msg = new DefaultBytesMessage(null);
    }


    public void readMessage() throws Exception {
        readMessageHead();
        readBody();
        while (currentOffset < totalLength) {
            readKeyValue();
        }
    }

    private void readMessageHead() throws Exception {
        int signature = readSignature();
        if (signature == SerialConfig.SIGNATURE_MESSAGE) {
            totalLength = readLength();
        } else throw new Exception("bad message first signature");
    }

    private void readBody() throws Exception {
        int signature = readSignature();
        if (signature == SerialConfig.SIGNATURE_BODY) {
            int bodyLength = readLength();
            System.out.println("body length " + bodyLength);
            msg.setBody(getStringBytes(bodyLength));
        } else throw new Exception("bad body signature, offset " + currentOffset);
    }

    private void readKeyValue() throws Exception {
        int keyValueHead = readSignature();
        if (keyValueHead == SerialConfig.SIGNATURE_HEADER || keyValueHead == SerialConfig.SIGNATURE_PROPERTY) {
            // pass the key signature
            readSignature();
            String key = readString();
            int signature = readSignature();
            switch (signature) {
                case SerialConfig.SIGNATURE_STRING:
                    if (keyValueHead == SerialConfig.SIGNATURE_HEADER) msg.putHeaders(key, readString());
                    else if (keyValueHead == SerialConfig.SIGNATURE_PROPERTY) msg.putProperties(key, readString());
                    else throw new Exception("bad key value signature, offset " + currentOffset);
                    break;
                case SerialConfig.SIGNATURE_INT:
                    if (keyValueHead == SerialConfig.SIGNATURE_HEADER) msg.putHeaders(key, readInt());
                    else if (keyValueHead == SerialConfig.SIGNATURE_PROPERTY) msg.putProperties(key, readInt());
                    else throw new Exception("bad key value signature, offset " + currentOffset);
                    break;
                case SerialConfig.SIGNATURE_LONG:
                    if (keyValueHead == SerialConfig.SIGNATURE_HEADER) msg.putHeaders(key, readLong());
                    else if (keyValueHead == SerialConfig.SIGNATURE_PROPERTY) msg.putProperties(key, readLong());
                    else throw new Exception("bad key value signature, offset " + currentOffset);
                    break;
                case SerialConfig.SIGNATURE_DOUBLE:
                    if (keyValueHead == SerialConfig.SIGNATURE_HEADER) msg.putHeaders(key, readDouble());
                    else if (keyValueHead == SerialConfig.SIGNATURE_PROPERTY) msg.putProperties(key, readDouble());
                    else throw new Exception("bad key value signature, offset " + currentOffset);
                    break;
                default:
                    throw new Exception("bad key value block, offset " + currentOffset);

            }
        } else throw new Exception("bad key value signature, current offset " + currentOffset);
    }


    public int readSignature() {
        return metaData[currentOffset++];
    }

    private int readLength() {
        int r = ((metaData[currentOffset] << 24 & 0xFF000000) |
                (metaData[currentOffset + 1] << 16 & 0x00FF0000) |
                (metaData[currentOffset + 2] << 8 & 0x0000FF00) |
                (metaData[currentOffset + 3] & 0x000000FF));
        System.out.println("length: " + r);
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

    public Message getMsg() {
        return this.msg;
    }

}
