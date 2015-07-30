package com.asiainno.base.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: KafkaBatchConsumer
 * @author: mingyu.zhao
 * @date: 15/6/26 下午5:12
 */
public class KafkaBatchConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchConsumer.class);

    private ConsumerConnector consumer;
    private ConsumerIterator<byte[], byte[]> it;

    private String zkConnect; //mandatory zookeeper01:2181,zookeeper02:2181,zookeeper03:2181/pengpeng/kafka
    private String topic; //mandatory
    private String groupId; //mandatory
    private String consumerId; //optional
    private boolean autoCommit = true; //optional

    private String consumerDesc;

    private int batchUpperLimit = 500;

    private int timeUpperLimit = 1000; //milliseconds
    private int consumerTimeout = 10; //milliseconds
    private int sleepWhenIdle = 2000; //milliseconds

    private volatile boolean running = true;
    private Object shutdownLck = new Object();

    private MessageConsumer messageConsumer;
    private final List messageList = new ArrayList();

    public KafkaBatchConsumer() {

    }

    public Status consume() {
        byte[] kafkaMessage;
        byte[] kafkaKey;
        long batchStartTime = System.currentTimeMillis();
        long batchEndTime = System.currentTimeMillis() + timeUpperLimit;

        try {
            boolean iterStatus = false;
            long startTime = System.nanoTime();
            while (messageList.size() < batchUpperLimit && System.currentTimeMillis() < batchEndTime) {
                iterStatus = hasNext();
                if (iterStatus) {
                    // get next message
                    MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
                    kafkaMessage = messageAndMetadata.message();
                    kafkaKey = messageAndMetadata.key();

                    MessageEvent msgEvent = new MessageEvent(kafkaMessage);
                    msgEvent.putHeader(MessageEvent.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
                    msgEvent.putHeader(MessageEvent.TOPIC, topic);
                    msgEvent.putHeader(MessageEvent.PARTITION, messageAndMetadata.partition());
                    msgEvent.putHeader(MessageEvent.OFFSET, messageAndMetadata.offset());
                    if (kafkaKey != null) {
                        msgEvent.putHeader(MessageEvent.KEY, new String(kafkaKey));
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("{}->Message: {}", consumerDesc, new String(kafkaMessage));
                    }

                    Object decode = messageConsumer.decode(msgEvent);
                    if (decode != null) {
                        messageList.add(decode);
                    }
                }

                if (log.isDebugEnabled()) {
                    log.debug("{}->Waited: {} Messages: {}", consumerDesc, System.currentTimeMillis() - batchStartTime, messageList.size());
                }
            }

            if (messageList.size() > 0) {
                //TODO:do consume messageList
                messageConsumer.consumeAll(messageList);

                if (log.isDebugEnabled()) {
                    log.debug("{}->Consumed messages {}", consumerDesc, messageList.size());
                }

                messageList.clear();

                if (!autoCommit) {
                    // commit the read transactions to Kafka to avoid duplicates
                    consumer.commitOffsets();
                }
            }

            if (!iterStatus) {
                return Status.BACKOFF;
            }

            return Status.READY;
        } catch (Throwable e) {
            log.error(consumerDesc + " Kafka consume EXCEPTION", e);
            return Status.BACKOFF;
        }
    }

    public synchronized void start() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zkConnect);
        props.setProperty("auto.commit.enable", String.valueOf(autoCommit));
        props.setProperty("consumer.timeout.ms", String.valueOf(consumerTimeout));
        props.setProperty("group.id", groupId);
        if (consumerId != null) {
            consumerId = genUniqueConsumerID(consumerId);
        }
        props.setProperty("consumer.id", consumerId);

        consumerDesc = groupId + "@" + topic;

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<>();
        // We always have just one topic being read by one thread
        topicCountMap.put(topic, 1);

        // Get the message iterator for our topic
        // Note that this succeeds even if the topic doesn't exist
        // in that case we simply get no messages for the topic
        // Also note that currently we only support a single topic
        try {
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> topicList = consumerMap.get(topic);
            KafkaStream<byte[], byte[]> stream = topicList.get(0);
            it = stream.iterator();
        } catch (Exception e) {
            throw e;
        }

        Thread t1 = new Thread(this);
        t1.setName(messageConsumer.getClass().getSimpleName() + "-main");
        t1.start();

        log.info("KafkaBatchConsumer started consumer:{}", consumerDesc);
    }

    public synchronized void stop() {
        if (consumer != null) {
            // exit cleanly. This syncs offsets of messages read to ZooKeeper
            // to avoid reading the same messages again
            consumer.shutdown();
        }
        synchronized (shutdownLck) {
            running = false; //shutdown controller thread
            try {
                shutdownLck.wait(10000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("KafkaBatchConsumer stopped consumer:{}", consumerDesc);
    }

    /**
     * Check if there are messages waiting in Kafka,
     * waiting until timeout (10ms by default) for messages to arrive.
     * and catching the timeout exception to return a boolean
     */
    boolean hasNext() {
        try {
            it.hasNext();
            return true;
        } catch (ConsumerTimeoutException e) {
            return false;
        }
    }

    private String genUniqueConsumerID(String consumerId) {
        int pid = KafkaUtils.getPid();
        String host = KafkaUtils.getHost();

        return consumerId + "-" + host + "-" + pid;
    }

    public void setZkConnect(String zkConnect) {
        this.zkConnect = zkConnect;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public void setBatchUpperLimit(int batchUpperLimit) {
        this.batchUpperLimit = batchUpperLimit;
    }

    public void setTimeUpperLimit(int timeUpperLimit) {
        this.timeUpperLimit = timeUpperLimit;
    }

    public void setConsumerTimeout(int consumerTimeout) {
        this.consumerTimeout = consumerTimeout;
    }

    public void setMessageConsumer(MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void run() {
        while (running) {
            Status status = consume();
            if (status == Status.BACKOFF) {
                log.info("consumer:{} no messages, sleep {}ms. ", consumerDesc, sleepWhenIdle);
                try {
                    Thread.sleep(sleepWhenIdle);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        synchronized (shutdownLck) {
            shutdownLck.notifyAll();
        }
    }

    public static enum Status {
        READY, BACKOFF
    }
}
