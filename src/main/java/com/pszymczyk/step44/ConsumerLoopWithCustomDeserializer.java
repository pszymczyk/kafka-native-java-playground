package com.pszymczyk.step44;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
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
public class ConsumerLoopWithCustomDeserializer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerLoopWithCustomDeserializer.class);

    private final KafkaConsumer<String, Customer> consumer;
    private final String topic;

    public ConsumerLoopWithCustomDeserializer(String groupId, String topic) {
        this.topic = topic;
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
        props.put(SESSION_TIMEOUT_MS_CONFIG, 5000);
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        try {
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
        } catch (WakeupException wakeupException) {
            logger.info("Handling WakeupException.");
        } finally {
            logger.info("Closing Kafka consumer...");
            consumer.close();
            logger.info("Kafka consumer closed.");
        }
    }

    public void wakeup() {
        consumer.wakeup();
    }
}
