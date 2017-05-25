package io.openmessaging.mydemo;

import io.openmessaging.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright (c) 2017 XiaoMi Inc. All Rights Reserved.
 * Authors: liujinhong <liujinhong@xiaomi.com>.
 * Created on 2017/5/9.
 */
public class MessagePool {
    //消息池中可以包含的最大消息数
    public static final int MAX_SIZE_OF_POOL = 1024 * 2;

    private int size;

    private int offset = 0;

    private List<Message> messages = null;

    public MessagePool() {
        this.messages = new ArrayList<>();
        this.size = 0;
    }

    public MessagePool(List<Message> messages) {
        this.messages = messages;
        this.size = messages.size();
    }

    public int getMessagePoolSize() {
        return this.size;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
        this.size = messages.size();
    }
}
