package com.pszymczyk.step5;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Step5Runner {

    protected static Logger logger = LoggerFactory.getLogger(Step5Runner.class);

    public static void main(String[] args) {
        String topic = "step5";
        ExecutorService executor = Executors.newSingleThreadExecutor();
        FirstLevelCacheBackedByKafka firstLevelCacheBackedByKafka = new FirstLevelCacheBackedByKafka(topic);
        executor.submit(firstLevelCacheBackedByKafka);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            firstLevelCacheBackedByKafka.shutdown();
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        while (true) {
            logger.info("Cached items:");
            logger.info(""+firstLevelCacheBackedByKafka.getCachedItems());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
