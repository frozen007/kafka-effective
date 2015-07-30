package com.asiainno.base.kafka;

import com.myz.base.kafka.KafkaBatchConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: TestKafkaBatchConsumer
 * @author: mingyu.zhao
 * @date: 15/6/27 上午9:39
 */
public class TestKafkaBatchConsumer {
    private static final Logger log = LoggerFactory.getLogger(TestKafkaBatchConsumer.class);

    static KafkaBatchConsumer consumer;

    static {
        consumer = new KafkaBatchConsumer();
        consumer.setZkConnect("localhost:2181");
        consumer.setTopic("test1");
        consumer.setGroupId("kafka-batch-consumers");
        consumer.setConsumerId("ff");
        consumer.setAutoCommit(false);
        consumer.setConsumerTimeout(500);
        consumer.start();
    }

    public static void main(String[] args) {
        test001();
    }

    public static void test001() {
        while(true) {
            KafkaBatchConsumer.Status status = consumer.consume();
            if (status == KafkaBatchConsumer.Status.BACKOFF) {
                log.info("Waiting messages...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
