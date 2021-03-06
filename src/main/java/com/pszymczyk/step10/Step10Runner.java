package com.pszymczyk.step10;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class Step10Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step10Runner.class);

    public static void main(String[] args) {
        String inputTopic = "loan-application-requests";
        String outputTopic = "loan-application-decisions";
        String groupId = "step10";

        var consumer = new LoanApplicationProcess(inputTopic, outputTopic, groupId, new DebtorsRepository() {});
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
