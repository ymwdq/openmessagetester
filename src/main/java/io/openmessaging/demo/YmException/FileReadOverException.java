package io.openmessaging.demo.YmException;

/**
 * Created by YangMing on 2017/5/27.
 */
public class FileReadOverException extends RuntimeException {
    private String msg;

    public FileReadOverException(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return this.msg;
    }
}
