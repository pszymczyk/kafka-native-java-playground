package com.pszymczyk.step3;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerLoopManualCommit implements AutoCloseable {

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
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        consumer.subscribe(List.of(topic));

        var lastConsumedOffsetsOnPartitions = new HashMap<Integer, Long>();

        while (true) {
            var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (var record : records) {
                Map<String, Object> data = new HashMap<>();
                data.put("partition", record.partition());
                data.put("offset", record.offset());
                data.put("value", record.value());
                lastConsumedOffsetsOnPartitions.put(record.partition(), record.offset());
                logger.info("Consumer id: {}, ConsumerRecord: {}", this.id, data);
            }

            for (var partitionAndOffset : lastConsumedOffsetsOnPartitions.entrySet()) {
                if (partitionAndOffset.getValue() % 5 == 0) {
                    Map<TopicPartition, OffsetAndMetadata> offsetToCommit = Map.of(
                            new TopicPartition(topic, partitionAndOffset.getKey()),
                            //The committed offset should always be the offset of the next message that your application will read
                            new OffsetAndMetadata(partitionAndOffset.getValue() + 1));
                    logger.info("Commit offset {} on partition {}", partitionAndOffset.getValue()+1, partitionAndOffset.getKey());
                    consumer.commitSync(offsetToCommit);
                }
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
