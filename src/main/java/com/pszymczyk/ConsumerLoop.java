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
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@SuppressWarnings("Duplicates")
public class ConsumerLoop {

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
        props.put(GROUP_INSTANCE_ID_CONFIG, "local-1");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumer id: {}, ConsumerRecord: {}",
                            this.id,
                            Map.of(
                                    "partition", record.partition(),
                                    "offset", record.offset(),
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
