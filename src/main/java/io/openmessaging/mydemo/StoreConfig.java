package io.openmessaging.mydemo;

/**
 * Created by YangMing on 2017/5/16.
 */
public class StoreConfig {
    public static final long MAX_BUFFER_SIZE = 30 * 1024L * 1024L;
    public static final long MAX_FILE_SIZE = 2L * 1024L * 1024L * 1024L;
    public static final int DATA_CHUNK_SIZE = 256 * 1024 * 1024;
    public static final String STORE_PATH = "/Users/liujinhong/";
    public static final String FILE_NAME = "testWrite";
}
