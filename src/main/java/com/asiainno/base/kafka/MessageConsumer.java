package com.asiainno.base.kafka;

import java.util.List;

/**
 * @ClassName: MessageConsumer
 * @author: mingyu.zhao
 * @date: 15/6/29 下午3:28
 */
public interface MessageConsumer<T> {

    T decode(MessageEvent msgEvent);

    void consumeAll(List<T> msgList);
}
