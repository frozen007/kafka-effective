package com.asiainno.base.kafka;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: MessageEvent
 * @author: mingyu.zhao
 * @date: 15/6/26 下午7:14
 */
public class MessageEvent {
    public static final String TIMESTAMP = "timestamp";
    public static final String TOPIC = "topic";
    public static final String OFFSET = "offset";
    public static final String PARTITION = "partition";
    public static final String KEY = "key";

    private Map<String, Object> headers = new HashMap<>();
    private byte[] content;

    public MessageEvent(byte[] content) {
        this.content = content;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void putHeader(String key, Object value) {
        this.headers.put(key, value);
    }

    public byte[] getContent() {
        return content;
    }

}
