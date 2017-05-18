package io.openmessaging.demo.YmSerial;

/**
 * Created by YangMing on 2017/5/8.
 */
public class SerialConfig {
    public static int SIGNATURE_BYTE_NUM = 1;
    /*
    标记后面紧跟长度，用8个字节表示
    0x00 表message开始
    0x01 表校验位
    0x02 表header类型
    0x03 表property类型
    0x04 表body类型
    0x05 表key类型
    0x06 表value类型
    0x07 表string类型
    0x08 表int类型
    0x09 表long类型
    0x0a 表示double类型
    0x0b 表string类型
     */
    public static final int SIGNATURE_MESSAGE = 0;
    public static final int SIGNATURE_HEADER = 2;
    public static final int SIGNATURE_PROPERTY = 3;
    public static final int SIGNATURE_STRING = 7;
    public static final int SIGNATURE_INT = 8;
    public static final int SIGNATURE_LONG = 9;
    public static final int SIGNATURE_DOUBLE = 10;
    public static final int SIGNATURE_BODY = 4;

    public static final int BLOCK_LENGTH_NUM = 4;

    // 变动时，注意是否溢出
    public static final int MAX_MESSAGE_SIZE = 256 * 1024;
    public static final int HEADER_SIZE = 1;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int DOUBLE_SIZE = 8;

}
