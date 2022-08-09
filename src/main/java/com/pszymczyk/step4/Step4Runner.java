package com.pszymczyk.step4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class Step4Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step4Runner.class);

    public static void main(String[] args) {
        String groupId = "step4";
        String topic = "step4";

        var consumer = new ConsumerLoopWithCustomDeserializer(groupId, topic);
        var kafkaConsumerThread = new Thread(consumer::start, "kafka-consumer-thread");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Hello Kafka consumer, wakeup!");
            consumer.wakeup();
            try {
                kafkaConsumerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "shutdown-hook-thread"));

        logger.info("Starting Kafka consumer thread.");
        kafkaConsumerThread.start();
        logger.info("Kafka consumer thread started.");
    }
}
