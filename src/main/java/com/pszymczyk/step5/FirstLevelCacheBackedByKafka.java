package com.pszymczyk.step5;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@SuppressWarnings("Duplicates")
public class FirstLevelCacheBackedByKafka {

    private static final Logger logger = LoggerFactory.getLogger(FirstLevelCacheBackedByKafka.class);
    private static final Map<String, String> cache = new HashMap<>();

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public FirstLevelCacheBackedByKafka(String topic) {
        this.topic = topic;
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, "step5_" + Optional.ofNullable(System.getProperty("INSTANCE_ID")).orElse("0"));
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(SESSION_TIMEOUT_MS_CONFIG, 5000);
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    cache.put(record.key(), record.value());
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

    public static Map<String, String> getCachedItems() {
        return Map.copyOf(cache);
    }

    public void wakeup() {
        consumer.wakeup();
    }
}
