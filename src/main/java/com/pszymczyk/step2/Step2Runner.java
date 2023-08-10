package com.pszymczyk.step2;

import com.pszymczyk.ConsumerLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.pszymczyk.Utils.wakeUpConsumer;

@SuppressWarnings("Duplicates")
public class Step2Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step2Runner.class);

    public static void main(String[] args) {
        var groupId = "step2";
        var topic = "step2";

        var consumer0 = new ConsumerLoop(groupId, topic);
        var consumer1 = new ConsumerLoop(groupId, topic);
        var consumer2 = new ConsumerLoop(groupId, topic);

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
    }
}
