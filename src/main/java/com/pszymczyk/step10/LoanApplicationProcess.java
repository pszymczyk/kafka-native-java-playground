package com.pszymczyk.step10;

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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;


public class LoanApplicationProcess implements Runnable {

    private final KafkaConsumer<String, LoanApplicationRequest> consumer;
    private final KafkaProducer<String, LoanApplicationDecision> producer;
    private final String loanApplicationRequestsTopic;
    private final String loanApplicationDecisionsTopic;
    private final String groupId;
    private final DebtorsRepository debtorsRepository;


    public LoanApplicationProcess(String loanApplicationRequestsTopic, String loanApplicationDecisionsTopic, String groupId,
                                  DebtorsRepository debtorsRepository) {
        this.loanApplicationRequestsTopic = loanApplicationRequestsTopic;
        this.loanApplicationDecisionsTopic = loanApplicationDecisionsTopic;
        this.groupId = groupId;
        this.debtorsRepository = debtorsRepository;

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LoanApplicationDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(consumerProperties);

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LoanApplicationDecisionSerializer.class.getName());
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
                ConsumerRecords<String, LoanApplicationRequest> loanApplicationRequests = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                if (!loanApplicationRequests.isEmpty()) {
                    producer.beginTransaction();
                    try {
                        List<ProducerRecord<String, LoanApplicationDecision>> outputRecords = processApplications(loanApplicationRequests);
                        for (ProducerRecord<String, LoanApplicationDecision> outputRecord : outputRecords) {
                            producer.send(outputRecord);
                        }
                        producer.sendOffsetsToTransaction(getUncommittedOffsets(loanApplicationRequests), groupId);
                        producer.commitTransaction();
                    } catch (Exception e) {
                        producer.abortTransaction();
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

    private Map<TopicPartition, OffsetAndMetadata> getUncommittedOffsets(ConsumerRecords<String, LoanApplicationRequest> records) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, LoanApplicationRequest>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        return offsetsToCommit;
    }

    private List<ProducerRecord<String, LoanApplicationDecision>> processApplications(ConsumerRecords<String, LoanApplicationRequest> loanApplicationRequests) {
        List<ProducerRecord<String, LoanApplicationDecision>> loanApplicationDecisions = new ArrayList<>();
        for (ConsumerRecord<String, LoanApplicationRequest> loanApplicationRequestConsumerRecord : loanApplicationRequests) {
            LoanApplicationRequest loanApplicationRequest = loanApplicationRequestConsumerRecord.value();
            if (debtorsRepository.getDebtors().contains(loanApplicationRequest.getRequester())) {
                submitDecision(loanApplicationDecisions, loanApplicationRequest.getAmount().multiply(new BigDecimal("0.5")),
                    loanApplicationRequest.getRequester());
            } else if (debtorsRepository.getBlackList().contains(loanApplicationRequest.getRequester())) {
                submitDecision(loanApplicationDecisions, BigDecimal.ZERO, loanApplicationRequest.getRequester());
            } else {
                submitDecision(loanApplicationDecisions, loanApplicationRequest.getAmount(), loanApplicationRequest.getRequester());
            }
        }

        return loanApplicationDecisions;
    }

    void submitDecision(List<ProducerRecord<String, LoanApplicationDecision>> loanApplicationDecisions,
                        BigDecimal amount,
                        String requester) {
        LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
        loanApplicationDecision.setAmount(amount);
        loanApplicationDecision.setRequester(requester);
        ProducerRecord<String, LoanApplicationDecision> loanApplicationDecisionProducerRecord = new ProducerRecord<>(loanApplicationDecisionsTopic,
            loanApplicationDecision);
        loanApplicationDecisions.add(loanApplicationDecisionProducerRecord);
    }


    public void shutdown() {
        consumer.wakeup();
    }
}
