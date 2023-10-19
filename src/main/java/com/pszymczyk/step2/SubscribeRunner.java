package com.pszymczyk.step2;

import com.pszymczyk.ConsumerLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.pszymczyk.Utils.wakeUpConsumer;

@SuppressWarnings("Duplicates")
public class SubscribeRunner {

    private static final Logger logger = LoggerFactory.getLogger(SubscribeRunner.class);

    public static void main(String[] args) {
        var groupId = "step2";
        var topic = "step2";

        //TODO 1/3 Create Kafka Consumers
        ConsumerLoop consumer0 = null;
        ConsumerLoop consumer1 = null;
        ConsumerLoop consumer2 = null;

        //TODO 2/3 Create Thread for each Consumer instance
        Thread consumer0Thread = null;
        Thread consumer1Thread = null;
        Thread consumer2Thread = null;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            wakeUpConsumer("0", consumer0, consumer0Thread);
            wakeUpConsumer("1", consumer1, consumer1Thread);
            wakeUpConsumer("2", consumer2, consumer2Thread);
        }, "shutdown-hook-thread"));

        // TODO 3/3 Start Consumer threads
    }
}
