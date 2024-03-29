package com.pszymczyk;

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

@SuppressWarnings("Duplicates")
public class ConsumerLoop {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerLoop.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public ConsumerLoop(String groupId, String topic) {
        this.topic = topic;
        var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumer thread: {}, ConsumerRecord: {}",
                        Thread.currentThread().getName(),
                        Map.of(
                            "partition", record.partition(),
                            "offset", record.offset(),
                            "key", Objects.toString(record.key()),
                            "value", record.value()));
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
