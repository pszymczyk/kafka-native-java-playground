package com.pszymczyk.step11;


import com.pszymczyk.Utils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FailingSplitter {

    private static final Logger logger = LoggerFactory.getLogger(FailingSplitter.class);

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final String loanApplicationRequestsTopic;
    private final String loanApplicationDecisionsTopic;
    private final String groupId;

    public FailingSplitter(String loanApplicationRequestsTopic, String loanApplicationDecisionsTopic, String groupId) {
        this.loanApplicationRequestsTopic = loanApplicationRequestsTopic;
        this.loanApplicationDecisionsTopic = loanApplicationDecisionsTopic;
        this.groupId = groupId;

        var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(consumerProperties);

        var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "failing_splitter_0");
        this.producer = new KafkaProducer<>(producerProperties);
    }

    public void start() {
        producer.initTransactions();
        consumer.subscribe(List.of(loanApplicationRequestsTopic));
        while (true) {
            var loanApplicationRequests = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> request : loanApplicationRequests) {
                producer.beginTransaction();
                try {
                    for (var singleChar : request.value().split("\\.")) {
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(loanApplicationDecisionsTopic, singleChar);
                        producer.send(producerRecord);
                    }
                    producer.sendOffsetsToTransaction(
                        Map.of(new TopicPartition(request.topic(), request.partition()),
                            new OffsetAndMetadata(request.offset())), new ConsumerGroupMetadata(groupId));
                    Utils.failSometimes();
                    producer.commitTransaction();
                } catch (Exception e) {
                    logger.error("Exception during transaction!", e);
                    producer.abortTransaction();
                    consumer.seek(new TopicPartition(request.topic(), request.partition()), request.offset());
                }
            }
        }
    }

    public void wakeup() {
        consumer.wakeup();
    }
}
