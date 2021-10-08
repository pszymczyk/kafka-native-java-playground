package com.pszymczyk.step5;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class Step5Runner {

    protected static Logger logger = LoggerFactory.getLogger(Step5Runner.class);

    public static void main(String[] args) {
        var topic = "step5";
        var executor = Executors.newFixedThreadPool(3);
        executor.submit(() -> {
            try (var firstLevelCacheByKafka = new FirstLevelCacheBackedByKafka(topic)){
                firstLevelCacheByKafka.start();
            }
        });

        while (true) {
            logger.info("Cache: {}",FirstLevelCacheBackedByKafka.getCachedItems());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
