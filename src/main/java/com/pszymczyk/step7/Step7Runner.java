package com.pszymczyk.step7;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Step7Runner {

    public static void main(String[] args) {
        String topic = "step7";
        SimpleKafkaProducerWithCustomPartitioner simpleKafkaProducer = new SimpleKafkaProducerWithCustomPartitioner(topic);

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
