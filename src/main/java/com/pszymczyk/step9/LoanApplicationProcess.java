package com.pszymczyk.step9;

import com.pszymczyk.Utils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class LoanApplicationProcess {

    private static final Logger logger = LoggerFactory.getLogger(LoanApplicationProcess.class);

    private final KafkaConsumer<String, LoanApplicationRequest> consumer;
    private final KafkaProducer<String, LoanApplicationDecision> producer;
    private final String loanApplicationRequestsTopic;
    private final String loanApplicationDecisionsTopic;
    private final String groupId;
    private final DebtorsRepository debtorsRepository;


    public LoanApplicationProcess(String loanApplicationRequestsTopic,
                                  String loanApplicationDecisionsTopic,
                                  String groupId,
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
        this.consumer = new KafkaConsumer<>(consumerProperties);

        var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LoanApplicationDecisionSerializer.class.getName());
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "loan_application_requests_0");
        this.producer = new KafkaProducer<>(producerProperties);

    }

    public void start() {
        producer.initTransactions();
        consumer.subscribe(List.of(loanApplicationRequestsTopic));
        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (var record : records) {
                    producer.beginTransaction();
                    //executing sendOffsetsToTransaction(...) blocks polling message with given offset for other members within group
                    producer.sendOffsetsToTransaction(getOffsets(record), new ConsumerGroupMetadata(groupId));
                    try {
                        LoanApplicationDecision loanApplicationDecision = processApplication(record.value());
                        producer.send(new ProducerRecord<>(loanApplicationDecisionsTopic, loanApplicationDecision));

                        Utils.failSometimes();
                        producer.commitTransaction();
                    } catch (Exception e) {
                        logger.error("Something wrong happened!", e);
                        producer.abortTransaction();
                        consumer.seek(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        break;
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

    private static Map<TopicPartition, OffsetAndMetadata> getOffsets(ConsumerRecord<String, LoanApplicationRequest> record) {
        return Map.of(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
    }

    private LoanApplicationDecision processApplication(LoanApplicationRequest loanApplicationRequest) {
        LoanApplicationDecision loanApplicationDecision;
        if (debtorsRepository.getDebtors().contains(loanApplicationRequest.getRequester())) {
            loanApplicationDecision = submitDecision(loanApplicationRequest.getAmount().multiply(new BigDecimal("0.5")),
                    loanApplicationRequest.getRequester());
        } else if (debtorsRepository.getBlackList().contains(loanApplicationRequest.getRequester())) {
            loanApplicationDecision = submitDecision(BigDecimal.ZERO, loanApplicationRequest.getRequester());
        } else {
            loanApplicationDecision = submitDecision(loanApplicationRequest.getAmount(), loanApplicationRequest.getRequester());
        }
        return loanApplicationDecision;
    }

    LoanApplicationDecision submitDecision(BigDecimal amount, String requester) {
        LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
        loanApplicationDecision.setAmount(amount);
        loanApplicationDecision.setRequester(requester);
        return loanApplicationDecision;
    }

    public void wakeup() {
        consumer.wakeup();
    }
}
