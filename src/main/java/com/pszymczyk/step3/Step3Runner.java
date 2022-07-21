package com.pszymczyk.step3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

import static com.pszymczyk.Utils.wakeUpConsumer;

@SuppressWarnings("Duplicates")
public class Step3Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step3Runner.class);

    public static void main(String[] args) {
        var groupId = "step3";
        var topic = "step3";

        var consumer0 = new ConsumerLoopManualCommit(0, groupId, topic);
        var consumer1 = new ConsumerLoopManualCommit(1, groupId, topic);
        var consumer2 = new ConsumerLoopManualCommit(2, groupId, topic);

        var consumer0Thread = new Thread(consumer0::start, "consumer-0-thread");
        var consumer1Thread = new Thread(consumer1::start, "consumer-1-thread");
        var consumer2Thread = new Thread(consumer2::start, "consumer-2-thread");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            wakeUpConsumer("0", consumer0, consumer0Thread);
            wakeUpConsumer("1", consumer1, consumer1Thread);
            wakeUpConsumer("2", consumer2, consumer2Thread);
        }, "shutdown-hook-thread"));

        logger.info("Starting consumer thread 0...");
        consumer0Thread.start();
        logger.info("Consumer thread 0 started.");
        logger.info("Starting consumer thread 1...");
        consumer1Thread.start();
        logger.info("Consumer thread 1 started.");
        logger.info("Starting consumer thread 2...");
        consumer2Thread.start();
        logger.info("Consumer thread 2 started.");


        var executor = Executors.newFixedThreadPool(3);
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoopManualCommit(0, groupId, topic)) {
                consumerLoop.start();
            }
        });
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoopManualCommit(1, groupId, topic)) {
                consumerLoop.start();
            }
        });
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoopManualCommit(2, groupId, topic)) {
                consumerLoop.start();
            }
        });
    }
}
