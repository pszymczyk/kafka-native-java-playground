package com.pszymczyk.step10;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class LoanApplicationProcess implements AutoCloseable {

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

        var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LoanApplicationDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(consumerProperties);

        var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LoanApplicationDecisionSerializer.class.getName());
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "rental_car_process_0");
        this.producer = new KafkaProducer<>(producerProperties);

    }

    public void start() {
        producer.initTransactions();
        consumer.subscribe(List.of(loanApplicationRequestsTopic));
        while (true) {
            var loanApplicationRequestsRecords = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            var loanApplicationRequests = StreamSupport.stream(loanApplicationRequestsRecords.records(loanApplicationRequestsTopic).spliterator(), false)
                            .map(ConsumerRecord::value)
                            .collect(Collectors.toList());

            producer.beginTransaction();
            try {
                var loanApplicationDecisions = processApplications(loanApplicationRequests);
                for (var outputRecord : loanApplicationDecisions) {
                    producer.send(new ProducerRecord<>(loanApplicationDecisionsTopic, outputRecord));
                }
                producer.sendOffsetsToTransaction(getUncommittedOffsets(loanApplicationRequestsRecords), new ConsumerGroupMetadata(groupId));
                producer.commitTransaction();
            } catch (Exception e) {
                producer.abortTransaction();
            }
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getUncommittedOffsets(ConsumerRecords<String, LoanApplicationRequest> records) {
        var offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (var partition : records.partitions()) {
            var partitionedRecords = records.records(partition);
            var offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        return offsetsToCommit;
    }

    private List<LoanApplicationDecision> processApplications(List<LoanApplicationRequest> loanApplicationRequests) {
        var loanApplicationDecisions = new ArrayList<LoanApplicationDecision>();
        for (var loanApplicationRequest : loanApplicationRequests) {
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

    void submitDecision(List<LoanApplicationDecision> loanApplicationDecisions,
                        BigDecimal amount,
                        String requester) {
        LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
        loanApplicationDecision.setAmount(amount);
        loanApplicationDecision.setRequester(requester);
        loanApplicationDecisions.add(loanApplicationDecision);
    }


    @Override
    public void close() {
        consumer.close();
    }
}
