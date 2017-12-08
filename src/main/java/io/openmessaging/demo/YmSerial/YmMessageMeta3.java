package io.openmessaging.demo.YmSerial;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;
import io.openmessaging.demo.DefaultBytesMessage;

import java.util.function.BinaryOperator;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmMessageMeta3 implements BytesMessage{
    private byte[] metaData;
    private byte[] bodyData;
    private int currentOffset = 0;
    private int bodyLength = 0;
    private DefaultBytesMessage defaultBytesMessage;

    public YmMessageMeta3(byte[] body) {
        metaData = new byte[SerialConfig.MAX_MESSAGE_HEADER_SIZE];
        copyHeaderSignature(SerialConfig.SIGNATURE_MESSAGE);
        copyLengthBytes(0);
        copyBody(body);

        defaultBytesMessage = new DefaultBytesMessage(body);
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
        this.bodyData = body;

        defaultBytesMessage.setBody(body);

        return this;
    }

    @Override public KeyValue headers() {
        return defaultBytesMessage.headers();
    }

    @Override public KeyValue properties() {
        return defaultBytesMessage.properties();
    }

    @Override public Message putHeaders(String key, int value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_HEADER);
        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, this.metaData, currentOffset);
        copyHeaderSignature(SerialConfig.SIGNATURE_INT);
        copyInt(value);

        defaultBytesMessage.putHeaders(key, value);
        return this;
    }

    private void copyBody(byte[] body) {
        bodyData = body;
        bodyLength = body.length;
    }

    @Override public Message putHeaders(String key, long value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_HEADER);
        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, this.metaData, currentOffset);
        copyHeaderSignature(SerialConfig.SIGNATURE_LONG);
        copyLong(value);

        defaultBytesMessage.putHeaders(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_HEADER);
        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, this.metaData, currentOffset);
        copyHeaderSignature(SerialConfig.SIGNATURE_DOUBLE);
        copyDouble(value);

        defaultBytesMessage.putHeaders(key, value);
        return this;

    }

    @Override public Message putHeaders(String key, String value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_HEADER);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, metaData, currentOffset);
        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(value.length());
        copyString(value, metaData, currentOffset);

        defaultBytesMessage.putHeaders(key, value);

        return this;
    }

    @Override public Message putProperties(String key, int value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_PROPERTY);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, this.metaData, currentOffset);

        copyHeaderSignature(SerialConfig.SIGNATURE_INT);
        copyInt(value);

        defaultBytesMessage.putProperties(key, value);

        return this;
    }

    @Override public Message putProperties(String key, long value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_PROPERTY);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, this.metaData, currentOffset);

        copyHeaderSignature(SerialConfig.SIGNATURE_LONG);
        copyLong(value);

        defaultBytesMessage.putProperties(key, value);

        return this;
    }

    @Override public Message putProperties(String key, double value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_PROPERTY);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, this.metaData, currentOffset);

        copyHeaderSignature(SerialConfig.SIGNATURE_DOUBLE);
        copyDouble(value);

        defaultBytesMessage.putProperties(key, value);

        return this;

    }

    @Override public Message putProperties(String key, String value) {
        copyHeaderSignature(SerialConfig.SIGNATURE_PROPERTY);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(key.length());
        copyString(key, metaData, currentOffset);

        copyHeaderSignature(SerialConfig.SIGNATURE_STRING);
        copyLengthBytes(value.length());
        copyString(value, metaData, currentOffset);

        defaultBytesMessage.putProperties(key, value);

        return this;
    }

    public void copyLong(Long i){
        longToByte(i, metaData, currentOffset);
        currentOffset += 8;
    }

    public byte[] longToByte(long i, byte[] target, int offset){
        target[offset + 7] = (byte) (i & 0xFF);
        target[offset + 6] = (byte) (i >> 8 & 0xFF);
        target[offset + 5] = (byte) (i >> 16 & 0xFF);
        target[offset + 4] = (byte) (i >> 24 & 0xFF);
        target[offset + 3] = (byte) (i >> 32 & 0xFF);
        target[offset + 2] = (byte) (i >> 40 & 0xFF);
        target[offset + 1] = (byte) (i >> 48 & 0xFF);
        target[offset] = (byte) (i >> 56 & 0xFF);
        return target;

    }

    public void copyDouble(double i){
        doubleToByte(i, metaData, currentOffset);
        currentOffset += 8;
    }

    public byte[] doubleToByte(double i, byte[] target, int offset){
        long l = Double.doubleToRawLongBits(i);
        target[offset + 7] = (byte) (l & 0xFF);
        target[offset + 6] = (byte) (l >> 8 & 0xFF);
        target[offset + 5] = (byte) (l >> 16 & 0xFF);
        target[offset + 4] = (byte) (l >> 24 & 0xFF);
        target[offset + 3] = (byte) (l >> 32 & 0xFF);
        target[offset + 2] = (byte) (l >> 40 & 0xFF);
        target[offset + 1] = (byte) (l >> 48 & 0xFF);
        target[offset] = (byte) (l >> 56 & 0xFF);
        return target;
    }

    public static void main(String[] args) {
        YmMessageMeta3 msg = new YmMessageMeta3(new byte[128]);
        msg.putHeaders("int", 3);
        msg.putHeaders("long", 3l);
        msg.putHeaders("string", "queue");
        msg.putHeaders("double", 3d);
        msg.refreshBodyByte();
        msg.refreshLengthByte();
        System.out.println(msg.getRealMetaData());
        System.out.println(msg);
    }

}
