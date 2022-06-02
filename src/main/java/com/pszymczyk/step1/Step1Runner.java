package com.pszymczyk.step1;

import com.pszymczyk.ConsumerLoop;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Step1Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step1Runner.class);

    public static void main(String[] args) {
        var groupId = "step1";
        var topic = "step1";

        ConsumerLoop consumer = new ConsumerLoop(groupId, topic);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Hello Kafka consumer, it's time to wake up!");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            consumer.start();
        } catch (WakeupException wakeupException) {
            logger.info("We've got WakeupException, everything alright.");
        } finally {
            consumer.close();
        }
    }
}
