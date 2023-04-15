package com.pszymczyk.step7;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class VipClientsPartitioner implements Partitioner {

    private final Partitioner defaultPartitioner;

    VipClientsPartitioner() {
        this.defaultPartitioner = new DefaultPartitioner();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        requireNonNull(key);

        if (((String) key).toLowerCase().contains("vip")) {
            return 0;
        } else {
            int partition = defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
            return partition == 0 ? 1 : partition;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
