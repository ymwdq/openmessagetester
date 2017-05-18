package io.openmessaging.demo.Util;

/**
 * Created by ym on 17-5-16.
 */
public class MessageCounter {
    private long totalBytes = 0l;
    private static MessageCounter mc = new MessageCounter();
    private MessageCounter() {

    }

    public static MessageCounter getInstance() {
        return mc;
    }

    public synchronized void countMessage(int bytesNum) {
        totalBytes += bytesNum / (1024 * 1024);
        System.out.println("current bytes " + totalBytes + " MB");
    }
}
