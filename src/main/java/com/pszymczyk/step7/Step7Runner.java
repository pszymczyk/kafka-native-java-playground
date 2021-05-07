package com.pszymczyk.step7;

import com.pszymczyk.FirstLevelCacheBackedByKafka;
import com.pszymczyk.SimpleKafkaProducer;
import com.pszymczyk.SimpleKafkaProducerWithCustomPartioner;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Step7Runner {

    protected static Logger logger = LoggerFactory.getLogger(FirstLevelCacheBackedByKafka.class);

    public static void main(String[] args) {
        String topic = "step7";
        SimpleKafkaProducerWithCustomPartioner simpleKafkaProducer = new SimpleKafkaProducerWithCustomPartioner(topic);

        simpleKafkaProducer.syncSend("vip", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("VIP", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("exclusive vip", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("vip", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("vip", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("vip", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("super vip", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("vip better that other", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("VIP", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("VIP", "Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
        simpleKafkaProducer.syncSend("regular", "Non Vip message " + UUID.randomUUID(), 5, TimeUnit.SECONDS);
    }
}
