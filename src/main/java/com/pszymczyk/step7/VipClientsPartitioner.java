package com.pszymczyk.step7;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Cluster;

public class VipClientsPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        requireNonNull(key);

        if (((String) key).toLowerCase().contains("vip")) {
            return 0;
        } else {
            return BuiltInPartitioner.partitionForKey(keyBytes, cluster.partitionCountForTopic(topic));
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
