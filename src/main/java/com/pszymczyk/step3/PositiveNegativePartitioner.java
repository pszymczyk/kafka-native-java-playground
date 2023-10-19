package com.pszymczyk.step3;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PositiveNegativePartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        requireNonNull(value);
        String valueAsString = (String) value;

        if (valueAsString.equals("0")) {
            return 0;
        } else if (valueAsString.startsWith("-")){
            return 1;
        } else {
            return 2;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
