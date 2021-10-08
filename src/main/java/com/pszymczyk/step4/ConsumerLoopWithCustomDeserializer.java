package com.pszymczyk.step4;

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
public class ConsumerLoopWithCustomDeserializer implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(ConsumerLoopWithCustomDeserializer.class);

    private final KafkaConsumer<String, Customer> consumer;
    private final String topic;

    public ConsumerLoopWithCustomDeserializer(String groupId, String topic) {
        this.topic = topic;
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        while (true) {
            var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (var record : records) {
                Map<String, Object> data = new HashMap<>();
                data.put("partition", record.partition());
                data.put("offset", record.offset());
                data.put("value", record.value());
                logger.info("ConsumerRecord: {}", data);
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
