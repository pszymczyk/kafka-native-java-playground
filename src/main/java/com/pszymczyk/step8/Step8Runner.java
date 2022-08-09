package com.pszymczyk.step8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Step8Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step8Runner.class);

    public static void main(String[] args) {
        String topic = "step8";
        SimpleKafkaProducerWithInterceptor simpleKafkaProducer = new SimpleKafkaProducerWithInterceptor(topic);

        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("Some random message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
    }
}
