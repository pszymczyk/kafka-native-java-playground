package com.pszymczyk.step6;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Step6Runner {

    protected static Logger logger = LoggerFactory.getLogger(Step6Runner.class);

    public static void main(String[] args) throws InterruptedException {
        String topic = "step6";
        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(topic);

        simpleKafkaProducer.sendAndForget("Message sent in send and forget mode");

        RecordMetadata messageSentInSyncMode = simpleKafkaProducer.syncSend("Message sent in sync mode", 5, TimeUnit.SECONDS);
        assert messageSentInSyncMode != null;
        logger.info("Message sent in sync mode metadata: {}", messageSentInSyncMode);

        Future<RecordMetadata> messageSentInAsyncMode = simpleKafkaProducer.asyncSend("Message sent in async mode", (metadata, exception) -> {
            logger.info("Message sent in async mode metadata: {}", metadata);
            logger.info("Message sent in async mode exception: ", exception);
        });

        while (!messageSentInAsyncMode.isDone()) {
            Thread.sleep(100);
        }
    }
}
