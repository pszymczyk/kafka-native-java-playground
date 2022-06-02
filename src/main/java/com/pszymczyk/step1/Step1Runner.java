package com.pszymczyk.step1;

import com.pszymczyk.ConsumerLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Step1Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step1Runner.class);

    public static void main(String[] args) {
        var groupId = "step1";
        var topic = "step1";

        final ConsumerLoop consumer = new ConsumerLoop(groupId, topic);
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
