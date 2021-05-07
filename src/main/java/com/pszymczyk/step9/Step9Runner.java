package com.pszymczyk.step9;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Step9Runner {

    protected static Logger logger = LoggerFactory.getLogger(Step9Runner.class);

    public static void main(String[] args) throws InterruptedException {
        String topic = "step9";
        IdempotentKafkaProducer idempotentKafkaProducer = new IdempotentKafkaProducer(topic);

        for (int i=0; i<Integer.MAX_VALUE; i++) {
            idempotentKafkaProducer.asyncSend(i + ". random message");
            Thread.sleep(50);
        }
    }
}
