package com.pszymczyk.step11;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("Duplicates")
public class Step11Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step11Runner.class);

    public static void main(String[] args) throws InterruptedException {
        String inputTopic = "step11-input";
        String outputTopic = "step11-output";
        String groupId = "step11";

        var consumer = new FailingSplitter(inputTopic, outputTopic, groupId);
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
