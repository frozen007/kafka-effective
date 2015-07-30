package com.myz.base.kafka;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

/**
 * @ClassName: StringBasedKafkaProducerFactory
 * @author: mingyu.zhao
 * @date: 15/7/1 下午5:45
 */
public class StringBasedKafkaProducerFactory {
    private boolean singleton = true;

    private Properties props;

    private KafkaProducerWrapper<String, String> producer;

    public void setProps(Properties props) {
        this.props = props;
    }

    public KafkaProducerWrapper<String, String> getProducer() throws Exception {
        if (this.singleton) {
            return this.producer;
        } else {
            return createProducer();
        }

    }

    public void setSingleton(boolean singleton) {
        this.singleton = singleton;
    }

    private KafkaProducerWrapper<String, String> createProducer() {
        ProducerConfig config = new ProducerConfig(props);
        KafkaProducerWrapper<String, String> producerWrapper = new KafkaProducerWrapper<>();
        producerWrapper.setProducer(new Producer<String, String>(config));
        return producerWrapper;
    }

    public void init() throws Exception {
        if (this.singleton) {
            producer = createProducer();
        }
    }
}
