package com.pszymczyk.step3;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerLoopManualCommit implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(ConsumerLoopManualCommit.class);

    private final KafkaConsumer<String, String> consumer;
    private final int id;
    private final String topic;

    public ConsumerLoopManualCommit(int id, String groupId, String topic) {
        this.id = id;
        this.topic = topic;
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        var lastConsumedOffsetsOnPartitions = new HashMap<Integer, Long>();

        while (true) {
            throw new RuntimeException("Implement here!");
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
