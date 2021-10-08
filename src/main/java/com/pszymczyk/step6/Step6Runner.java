package com.pszymczyk.step6;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Step6Runner {

    protected static Logger logger = LoggerFactory.getLogger(Step6Runner.class);

    public static void main(String[] args) throws InterruptedException {
        var topic = "step6";
        var simpleKafkaProducer = new SimpleKafkaProducer(topic);

        simpleKafkaProducer.sendAndForget("Message sent in send and forget mode");

        var messageSentInSyncMode = simpleKafkaProducer.syncSend("Message sent in sync mode", 5, TimeUnit.SECONDS);
        assert messageSentInSyncMode != null;
        logger.info("Message sent in sync mode metadata: {}", messageSentInSyncMode);

        var messageSentInAsyncMode = simpleKafkaProducer.asyncSend("Message sent in async mode", (metadata, exception) -> {
            logger.info("Message sent in async mode metadata: {}", metadata);
            logger.info("Message sent in async mode exception: ", exception);
        });

        while (!messageSentInAsyncMode.isDone()) {
            Thread.sleep(100);
        }
    }
}
