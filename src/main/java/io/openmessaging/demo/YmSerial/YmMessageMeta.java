package io.openmessaging.demo.YmSerial;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

/**
 * Created by YangMing on 2017/5/8.
 */
public class YmMessageMeta implements BytesMessage {
    private byte[] metaData;
    private int current_offset = 0;

    public YmMessageMeta(byte[] body) {
        metaData = new byte[SerialConfig.MAX_MESSAGE_SIZE];
        copyHeaderSignature(SerialConfig.SIGNATURE_MESSAGE);
        copyLengthBytes(0);
        copyHeaderSignature(SerialConfig.SIGNATURE_BODY);
        copyLengthBytes(body.length);
        System.arraycopy(body, 0, metaData, current_offset, body.length);
        current_offset += body.length;

        refreshLength();
    }

    private void refreshLength() {
        intToByte4(current_offset, metaData, SerialConfig.SIGNATURE_BYTE_NUM);
    }

    public byte[] getMetaData() {
        copyTotalLength();
        return this.metaData;
    }

    public int getTotalLength() {
        return this.current_offset;
    }

    public void copyHeaderSignature(int signature) {
        intToByte1(signature, metaData, current_offset);
        current_offset += SerialConfig.HEADER_SIZE;
    }

    public void copyLengthBytes(int length) {
        intToByte4(length, metaData, current_offset);
        current_offset += SerialConfig.BLOCK_LENGTH_NUM;
    }

    public void copyInt(int i) {
        intToByte4(i, metaData, current_offset);
        current_offset += 4;
    }
    public byte[] intToByte4(int i, byte[] target, int offset) {
        target[offset + 3] = (byte) (i & 0xFF);
        target[offset + 2] = (byte) (i >> 8 & 0xFF);
        target[offset + 1] = (byte) (i >> 16 & 0xFF);
        target[offset] = (byte) (i >> 24 & 0xFF);
        return target;
    }

    public byte[] intToByte2(int i, byte[] target, int offset) {
        target[offset + 1] = (byte) (i & 0xFF);
        target[offset] = (byte) (i >> 8 & 0xFF);
        return target;
    }

    public byte[] intToByte1(int i, byte[] target, int offset) {
        target[offset] = (byte) (i & 0xFF);
        return target;
    }

    public void copyString(String s, byte[] target, int offset) {
        System.arraycopy(s.getBytes(), 0, target, offset, s.getBytes().length);
        current_offset += s.getBytes().length;
    }

    public void copyTotalLength() {
        intToByte4(current_offset, metaData, 1);
    }

    @Override public byte[] getBody() {
        return null;
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
        copyString(key, this.metaData, current_offset);
        copyHeaderSignature(SerialConfig.SIGNATURE_INT);
        copyInt(value);

        refreshLength();
        return this;
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
        copyString(key, metaData, current_offset);
        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(value.length());
        copyString(value, metaData, current_offset);

        refreshLength();
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        return null;
    }

    @Override public Message putProperties(String key, long value) {
        return null;
    }

    @Override public Message putProperties(String key, double value) {
        return null;
    }

    @Override public Message putProperties(String key, String value) {
        return null;
    }


    public static void main(String[] args) {
        byte[] test = {2, 3};
        YmMessageMeta msg = new YmMessageMeta(test);
        msg.putHeaders("111", 3);
        System.out.println(msg);
        System.out.println("over");
    }
}
