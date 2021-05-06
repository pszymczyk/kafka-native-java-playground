package com.pszymczyk.step1;

import com.pszymczyk.ConsumerLoop;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Step1Runner {

    public static void main(String[] args) {
        String groupId = "step1_1";
        String topic = "step1";
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ConsumerLoop consumer = new ConsumerLoop(groupId, topic);
        executor.submit(consumer);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.shutdown();
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }
}
