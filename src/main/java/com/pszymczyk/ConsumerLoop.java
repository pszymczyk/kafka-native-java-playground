package com.pszymczyk;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@SuppressWarnings("Duplicates")
public class ConsumerLoop implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerLoop.class);

    private final KafkaConsumer<String, String> consumer;
    private final int id;
    private final String topic;

    public ConsumerLoop(String groupId, String topic) {
        this(0, groupId, topic);
    }

    public ConsumerLoop(int id, String groupId, String topic) {
        this.id = id;
        this.topic = topic;
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> data = new HashMap<>();
                data.put("partition", record.partition());
                data.put("offset", record.offset());
                data.put("value", record.value());
                logger.info("Consumer id: {}, ConsumerRecord: {}", this.id, data);
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
