package com.pszymczyk.step8;

import com.pszymczyk.step7.SimpleKafkaProducerWithCustomPartioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Step8Runner {

    protected static Logger logger = LoggerFactory.getLogger(Step8Runner.class);

    public static void main(String[] args) {
        String topic = "step8";
        SimpleKafkaProducerWithSerializer simpleKafkaProducer = new SimpleKafkaProducerWithSerializer(topic);

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
