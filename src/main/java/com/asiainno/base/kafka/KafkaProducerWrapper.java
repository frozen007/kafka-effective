package com.asiainno.base.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * @ClassName: KafkaProducerWrapper
 * @author: mingyu.zhao
 * @date: 15/7/1 下午6:22
 */
public class KafkaProducerWrapper<K, V> {
    private Producer<K, V> producer;

    private String queueTopic;

    public void setProducer(Producer<K, V> producer) {
        this.producer = producer;
    }

    public void setQueueTopic(String queueTopic) {
        this.queueTopic = queueTopic;
    }

    public void send(K key, V value) {
        KeyedMessage<K, V> message = new KeyedMessage<>(queueTopic, key, value);
        producer.send(message);
    }
}
