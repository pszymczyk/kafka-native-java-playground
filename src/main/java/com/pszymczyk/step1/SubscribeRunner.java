package com.pszymczyk.step1;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SubscribeRunner {

    private static final String GROUP_ID = "step1";
    private static final String TOPIC = "step1";
    private static final Logger logger = LoggerFactory.getLogger(SubscribeRunner.class);

    public static void main(String[] args) {

        //Todo 1/3 - create Kafka Consumer
        final KafkaConsumer<String, String> kafkaConsumer = null;

        final var mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Hello Kafka consumer, wakeup!");
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Exception during application close.", e);
            }
        }, "shutdown-hook-thread"));


        //Todo 2/3 - subscribe

        try {
            //Todo 3/3 - poll records

        } catch (WakeupException wakeupException) {
            logger.info("Handling WakeupException.");
        } finally {
            logger.info("Closing Kafka consumer...");
            kafkaConsumer.close();
            logger.info("Kafka consumer closed.");
        }
    }
}
