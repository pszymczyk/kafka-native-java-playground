package com.pszymczyk.step11;


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
import java.util.*;

public class FailingSplitter implements Runnable {

    protected static Logger logger = LoggerFactory.getLogger(FailingSplitter.class);

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
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "rental_car_process_1");
        this.producer = new KafkaProducer<>(producerProperties);
    }

    @Override
    public void run() {
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
                    failSometimes();
                    producer.commitTransaction();
                } catch (Exception e) {
                    logger.error("Exception during transaction!", e);
                    producer.abortTransaction();
                    consumer.seek(new TopicPartition(request.topic(), request.partition()), request.offset());
                }
            }
        }
    }

    private void failSometimes() throws Exception {
        Random rand = new Random();
        int randomNum = rand.nextInt((3 - 1) + 1) + 1;
        logger.info("Random number: {}", randomNum);
        if (randomNum == 2) {
            throw new Exception("Random number 2 = exception!");
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getUncommittedOffsets(ConsumerRecords<String, String> records) {
        var offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (TopicPartition partition : records.partitions()) {
            var partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        return offsetsToCommit;
    }

    public void shutdown() {
        producer.close();
        consumer.close();
    }
}
