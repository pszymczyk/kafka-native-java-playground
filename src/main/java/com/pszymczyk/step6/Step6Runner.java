package com.pszymczyk.step6;

import io.confluent.examples.clients.basicavro.Payment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Step6Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step6Runner.class);

    public static void main(String[] args) throws InterruptedException {
        var topic = "payment";
        var simpleKafkaProducer = new SimpleKafkaProducer(topic);


        var payment = new Payment("123", 10.2d);
        var messageSentInSyncMode = simpleKafkaProducer.syncSend(payment, 5, TimeUnit.SECONDS);
        assert messageSentInSyncMode != null;
        logger.info("Message sent in sync mode metadata: {}", messageSentInSyncMode);
    }
}
