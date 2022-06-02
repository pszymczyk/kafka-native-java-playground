package com.pszymczyk.step2;

import com.pszymczyk.ConsumerLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class Step2Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step2Runner.class);

    public static void main(String[] args) {
        var groupId = "step2";
        var topic = "step2";

        var consumer0 = new ConsumerLoop(0, groupId, topic);
        var consumer1 = new ConsumerLoop(1, groupId, topic);
        var consumer2 = new ConsumerLoop(2, groupId, topic);

        var consumer0Thread = new Thread(consumer0::start, "consumer-0-thread");
        var consumer1Thread = new Thread(consumer1::start, "consumer-1-thread");
        var consumer2Thread = new Thread(consumer2::start, "consumer-2-thread");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Hello consumer 0, wakeup!");
            consumer0.wakeup();
            try {
                consumer0Thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Hello consumer 1, wakeup!");
            consumer1.wakeup();
            try {
                consumer1Thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Hello consumer 2, wakeup!");
            consumer2.wakeup();
            try {
                consumer2Thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "shutdown-hook-thread"));

        consumer0Thread.start();
        consumer1Thread.start();
        consumer2Thread.start();
    }
}
