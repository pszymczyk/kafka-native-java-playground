package com.pszymczyk.step5;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class Step5Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step5Runner.class);

    public static void main(String[] args) {
        var topic = "step5";
        var kafkaConsumer = new FirstLevelCacheBackedByKafka(topic);
        var kafkaConsumerThread = new Thread(kafkaConsumer::start, "kafka-consumer-thread");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Hello Kafka consumer, wakeup!");
            kafkaConsumer.wakeup();
            try {
                kafkaConsumerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "shutdown-hook-thread"));

        logger.info("Starting Kafka consumer thread.");
        kafkaConsumerThread.start();
        logger.info("Kafka consumer thread started.");

        while (true) {
            logger.info("Cache: {}",FirstLevelCacheBackedByKafka.getCachedItems());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
