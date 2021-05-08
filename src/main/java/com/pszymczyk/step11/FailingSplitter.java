package com.pszymczyk.step11;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;


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

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(consumerProperties);

        Properties producerProperties = new Properties();
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
        try {
            consumer.subscribe(Arrays.asList(loanApplicationRequestsTopic));
            while (true) {
                ConsumerRecords<String, String> loanApplicationRequests = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                if (!loanApplicationRequests.isEmpty()) {
                    producer.beginTransaction();
                    try {
                        List<ProducerRecord<String, String>> outputRecords = processApplications(loanApplicationRequests);
                        for (ProducerRecord<String, String> outputRecord : outputRecords) {
                            producer.send(outputRecord);
                        }
                        producer.sendOffsetsToTransaction(getUncommittedOffsets(loanApplicationRequests), groupId);

                        failSometimes();

                        producer.commitTransaction();
                    } catch (Exception e) {
                        producer.abortTransaction();
                        continue;
                    }

                }

            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private void failSometimes() throws Exception {
        Random rand = new Random();
        int randomNum = rand.nextInt((4 - 1) + 1) + 1;
        logger.info("Random number: {}", randomNum);
        if (randomNum == 2) {
            throw new Exception();
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getUncommittedOffsets(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        return offsetsToCommit;
    }

    private List<ProducerRecord<String, String>> processApplications(ConsumerRecords<String, String> loanApplicationRequests) {
        List<ProducerRecord<String, String>> producerRecords = new ArrayList<>();
        for (ConsumerRecord<String, String> record : loanApplicationRequests) {
            for (String singleChar: record.value().split("\\.")) {
                ProducerRecord<String, String> loanApplicationDecisionProducerRecord = new ProducerRecord<>(loanApplicationDecisionsTopic, singleChar);
                producerRecords.add(loanApplicationDecisionProducerRecord);
            }
        }

        return producerRecords;
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
