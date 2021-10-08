package com.pszymczyk.step5;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class FirstLevelCacheBackedByKafka implements AutoCloseable {

    private static final Map<String, String> cache = new HashMap<>();

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public FirstLevelCacheBackedByKafka(String topic) {
        this.topic = topic;
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, "step5_" + UUID.randomUUID().toString().substring(0, 7));
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                cache.put(record.key(), record.value());
            }
        }
    }

    public static Map<String, String> getCachedItems() {
        return Map.copyOf(cache);
    }

    @Override
    public void close() {
        consumer.wakeup();
    }


}
