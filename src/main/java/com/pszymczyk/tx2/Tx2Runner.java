package com.pszymczyk.tx2;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class Tx2Runner {

    private static final Logger logger = LoggerFactory.getLogger(Tx2Runner.class);
    private static final String inputTopic = "tx2-input";
    private static final String outputTopic = "tx2-output";
    private static final String groupId = "tx2-input";

    public static void main(String[] args) {

        //TODO 1/4 create and configure Kafka Consumer
        final var kafkaConsumer = createKafkaConsumer(groupId);

        //TODO 2/4 create and configure Kafka Producer
        final var kafkaProducer = createKafkaProducer();
        registerShutdownHook(kafkaProducer, kafkaConsumer);

        //TODO 3/4 init transactions and subscribe to topic

        while (true) {
            //TODO 4/4 implement transactional processing
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String groupId) {
        final var consumerProperties = new Properties();
        final var kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
        return kafkaConsumer;
    }

    private static KafkaProducer<String, BusinessTransaction> createKafkaProducer() {
        final var producerProperties = new Properties();
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
