package com.pszymczyk.step1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

class SubscribeRunner {

    private static final String GROUP_ID = "step1";
    private static final String TOPIC = "step1";
    private static final Logger logger = LoggerFactory.getLogger(SubscribeRunner.class);

    public static void main(String[] args) {

        final var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        props.put(GROUP_ID_CONFIG, GROUP_ID);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final var kafkaConsumer = new KafkaConsumer<String, String>(props);

        final var mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Hello Kafka consumer, wakeup!");
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Exception during application close.", e);
            }
        }, "shutdown-hook-thread"));


        kafkaConsumer.subscribe(List.of(TOPIC));

        try {
            while (true) {
                var records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("ConsumerRecord: {}", Map.of("partition", record.partition(), "offset", record.offset(), "key", Objects.toString(record.key()),
                        "value", record.value()));
                }
            }
        } catch (WakeupException wakeupException) {
            logger.info("Handling WakeupException.");
        } finally {
            logger.info("Closing Kafka consumer...");
            kafkaConsumer.close();
            logger.info("Kafka consumer closed.");
        }
    }
}
