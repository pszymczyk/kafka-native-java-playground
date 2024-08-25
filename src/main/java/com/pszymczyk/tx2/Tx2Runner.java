package com.pszymczyk.tx2;

import com.pszymczyk.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

class Tx2Runner {

    private static final Logger logger = LoggerFactory.getLogger(Tx2Runner.class);
    private static final String inputTopic = "tx2-input";
    private static final String outputTopic = "tx2-output";
    private static final String groupId = "tx2-input";

    public static void main(String[] args) {

        final var kafkaConsumer = createKafkaConsumer();
        final var kafkaProducer = createKafkaProducer();
        registerShutdownHook(kafkaProducer, kafkaConsumer);

        kafkaProducer.initTransactions();

        kafkaConsumer.subscribe(List.of(inputTopic));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (var record : records) {

                try {

                    kafkaProducer.beginTransaction();
                    String[] split = record.value().split(",");
                    String buyer = split[1];
                    String seller = split[2];
                    String stock = split[3];
                    int number = Integer.parseInt(split[4]);

                    kafkaProducer.sendOffsetsToTransaction(Map.of(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset())), kafkaConsumer.groupMetadata());

                    ProducerRecord<String, BusinessTransaction> buy = new ProducerRecord<>(
                            outputTopic, buyer, BusinessTransaction.buy(buyer, stock, number));
                    kafkaProducer.send(buy, (metadata, exception) -> {
                        if (metadata != null) {
                            logger.info("Record sent, {}.", metadata);
                        } else {
                            logger.error("Record sending failed. ", exception);
                        }
                    }).get();

                    Utils.failSometimes();

                    ProducerRecord<String, BusinessTransaction> sell = new ProducerRecord<>(
                            outputTopic, seller, BusinessTransaction.sell(seller, stock, number));
                    kafkaProducer.send(sell, (metadata, exception) -> {
                        if (metadata != null) {
                            logger.info("Record sent, {}.", metadata);
                        } else {
                            logger.error("Record sending failed. ", exception);
                        }
                    }).get();

                    kafkaProducer.commitTransaction();
                } catch (Exception exception) {
                    logger.error("Exception during message processing.", exception);
                    kafkaProducer.abortTransaction();
                    kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                    break;
                }
            }
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        final var consumerProperties = new Properties();
        consumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        consumerProperties.put(GROUP_ID_CONFIG, groupId);
        consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        final var kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
        return kafkaConsumer;
    }

    private static KafkaProducer<String, BusinessTransaction> createKafkaProducer() {
        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BusinessTransactionSerializer.class);
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "Tx2Runner_" + UUID.randomUUID());
        final var kafkaProducer = new KafkaProducer<String, BusinessTransaction>(producerProperties);
        return kafkaProducer;
    }

    private static void registerShutdownHook(KafkaProducer<String, BusinessTransaction> kafkaProducer, KafkaConsumer<String, String> kafkaConsumer) {
        final var mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaProducer.close();
            logger.info("Hello Kafka consumer, wakeup!");
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Exception during application close.", e);
            }
        }, "shutdown-hook-thread"));
    }
}
