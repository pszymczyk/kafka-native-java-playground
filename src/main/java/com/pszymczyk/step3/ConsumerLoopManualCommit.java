package com.pszymczyk.step3;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerLoopManualCommit {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerLoopManualCommit.class);

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
        props.put(GROUP_INSTANCE_ID_CONFIG, "local-1");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (var record : records) {
                    logger.info("Consumer id: {}, ConsumerRecord: {}",
                            this.id,
                            Map.of("partition", record.partition(),
                                    "offset", record.offset(),
                                    "value", record.value()));

                    if (record.offset() % 5 == 0) {
                        consumer.commitSync(Map.of(new TopicPartition(topic, record.partition()), new OffsetAndMetadata(record.offset())));
                        logger.info("Commit offset {} on partition {}", record.offset() + 1, record.partition());
                    }
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
