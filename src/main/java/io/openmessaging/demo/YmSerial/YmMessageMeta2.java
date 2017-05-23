package io.openmessaging.demo.YmSerial;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

/**
 * Created by YangMing on 2017/5/22.
 */
public class YmMessageMeta2 implements BytesMessage{
    private byte[] metaData;
    private byte[] bodyData;
    private int currentOffset = 0;
    private int bodyLength = 0;

    public YmMessageMeta2(byte[] body) {
        metaData = new byte[SerialConfig.MAX_MESSAGE_HEADER_SIZE];
        copyHeaderSignature(SerialConfig.SIGNATURE_MESSAGE);
        copyLengthBytes(0);
        copyBody(body);
    }


    public byte[] getRealMetaData() {
        // to optimize
        byte[] r = new byte[currentOffset];
        for (int i = 0; i < currentOffset; i++) {
            r[i] = metaData[i];
        }
        return r;
    }

    public void refreshLengthByte() {
        copyTotalLength();
    }

    public void refreshBodyByte() {
        copyHeaderSignature(SerialConfig.SIGNATURE_BODY);
        copyLengthBytes(bodyData.length);
    }

    public int getMetaDataLength() {
        return this.currentOffset;
    }

    public int getTotalLength() {
        return bodyLength + currentOffset;
    }

    private void copyHeaderSignature(int signature) {
        intToByte1(signature, metaData, currentOffset);
        currentOffset += SerialConfig.HEADER_SIZE;
    }

    private void copyLengthBytes(int length) {
        intToByte4(length, metaData, currentOffset);
        currentOffset += SerialConfig.BLOCK_LENGTH_NUM;
    }

    private void copyInt(int i) {
        intToByte4(i, metaData, currentOffset);
        currentOffset += 4;
    }

    private void copyBytes(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
        int i = srcOffset;
        int j = dstOffset;
        int cnt = 0;
        while (cnt < length) {
            dst[j] = src[i];
            i++;
            j++;
            cnt++;
        }
    }


    private byte[] intToByte4(int i, byte[] target, int offset) {
        target[offset + 3] = (byte) (i & 0xFF);
        target[offset + 2] = (byte) (i >> 8 & 0xFF);
        target[offset + 1] = (byte) (i >> 16 & 0xFF);
        target[offset] = (byte) (i >> 24 & 0xFF);
        return target;
    }

    private byte[] intToByte2(int i, byte[] target, int offset) {
        target[offset + 1] = (byte) (i & 0xFF);
        target[offset] = (byte) (i >> 8 & 0xFF);
        return target;
    }

    private byte[] intToByte1(int i, byte[] target, int offset) {
        target[offset] = (byte) (i & 0xFF);
        return target;
    }

    public void copyString(String s, byte[] target, int offset) {
        copyBytes(s.getBytes(), 0, target, offset, s.getBytes().length);
        currentOffset += s.getBytes().length;
    }

    public void copyTotalLength() {
        intToByte4(bodyLength + currentOffset, metaData, 1);
    }

    @Override public byte[] getBody() {
        return bodyData;
    }

    @Override public BytesMessage setBody(byte[] body) {
        return this;
    }

    @Override public KeyValue headers() {
        return null;
    }

    @Override public KeyValue properties() {
        return null;
    }

    @Override public Message putHeaders(String key, int value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_HEADER);
        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, this.metaData, currentOffset);
        copyHeaderSignature(SerialConfig.SIGNATURE_INT);
        copyInt(value);

        return this;
    }

    private void copyBody(byte[] body) {
        bodyData = body;
        bodyLength = body.length;
    }

    @Override public Message putHeaders(String key, long value) {
        return null;
    }

    @Override public Message putHeaders(String key, double value) {
        return null;
    }

    @Override public Message putHeaders(String key, String value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_HEADER);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, metaData, currentOffset);
        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(value.length());
        copyString(value, metaData, currentOffset);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_PROPERTY);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, this.metaData, currentOffset);

        copyHeaderSignature(SerialConfig.SIGNATURE_INT);
        copyInt(value);

        return this;
    }

    @Override public Message putProperties(String key, long value) {
        return null;
    }

    @Override public Message putProperties(String key, double value) {
        return null;
    }

    @Override public Message putProperties(String key, String value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_PROPERTY);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, metaData, currentOffset);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(value.length());
        copyString(value, metaData, currentOffset);
        return this;
    }


    public static void main(String[] args) {
        byte[] test = {2, 3};
        YmMessageMeta msg = new YmMessageMeta(test);
        msg.putHeaders("111", 3);
        System.out.println(msg);
        System.out.println("over");
    }
}
