package com.myz.base.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: KeyValueModPartitioner
 * @author: myz
 * @date: 15/9/24 上午11:29
 */
public class KeyValueModPartitioner implements Partitioner {
    private static final Logger log = LoggerFactory.getLogger(KeyValueModPartitioner.class);

    public KeyValueModPartitioner(VerifiableProperties props) {

    }

    @Override
    public int partition(Object key, int numPartitions) {
        long numValue = 0;
        try {
            numValue = Long.parseLong(key instanceof String ? (String) key : key.toString());
        } catch (Exception e) {
            log.error("error when parse key to long:" + key);
        }
        int par = (int) (numValue % numPartitions);
        return par;
    }
}
