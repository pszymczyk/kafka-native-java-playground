package com.pszymczyk.step11;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@SuppressWarnings("Duplicates")
public class ReadCommittedVsUncommittedConsumer {

    public static void main(String[] args) {
        String topic = "step11-output";
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            try (var readUncommitted = new ReadUncommitted(topic)) {
                readUncommitted.start();
            }
        });
        executor.submit(() -> {
            try (var readCommitted = new ReadCommitted(topic)) {
                readCommitted.start();
            }
        });
    }

    static public class ReadCommitted implements AutoCloseable {

        protected static Logger logger = LoggerFactory.getLogger(ReadCommitted.class);

        private final KafkaConsumer<String, String> consumer;
        private final String topic;


        public ReadCommitted(String topic) {
            this.topic = topic;
            var properties = new Properties();
            properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(GROUP_ID_CONFIG, "step5_" + UUID.randomUUID().toString().substring(0, 7));
            properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
            this.consumer = new KafkaConsumer<>(properties);
        }

        public void start() {
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("READ COMMITTED, record {}", record);
                }
            }
        }

        @Override
        public void close() {
            consumer.close();
        }
    }

    static public class ReadUncommitted implements AutoCloseable {

        protected static Logger logger = LoggerFactory.getLogger(ReadUncommitted.class);

        private final KafkaConsumer<String, String> consumer;
        private final String topic;


        public ReadUncommitted(String topic) {
            this.topic = topic;
            var properties = new Properties();
            properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(GROUP_ID_CONFIG, "step5_" + UUID.randomUUID().toString().substring(0, 7));
            properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT));
            this.consumer = new KafkaConsumer<>(properties);
        }

        public void start() {
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("READ UNCOMMITTED, record {}", record);
                }
            }
        }

        @Override
        public void close() {
            consumer.close();
        }
    }
}
