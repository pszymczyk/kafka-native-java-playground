package com.pszymczyk.step7;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.Map;
import java.util.Objects;

public class VipClientsPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Objects.requireNonNull(key);
        if (((String) key).toLowerCase().contains("vip")) {
            return 0;
        } else {
            return cluster.availablePartitionsForTopic(topic)
                .stream()
                .filter(partitionInfo -> partitionInfo.partition() > 0)
                .findAny()
                .map(PartitionInfo::partition)
                .orElseThrow(() -> new RuntimeException("No partition available for non vip clients!"));
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
