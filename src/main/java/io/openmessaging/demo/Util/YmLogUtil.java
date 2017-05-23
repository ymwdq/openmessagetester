package io.openmessaging.demo.Util;

/**
 * Created by YangMing on 2017/5/4.
 */
public class YmLogUtil {
    private long startTime;
    private long endTime;

    public void startCount() {
        this.startTime = System.currentTimeMillis();
    }

    public void endCount() {
        this.endTime = System.currentTimeMillis();
    }

    public void printTime() {
        System.out.println("cost time: " + (this.endTime - this.startTime));
    }
}
