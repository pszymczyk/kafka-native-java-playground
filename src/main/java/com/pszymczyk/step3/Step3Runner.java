package com.pszymczyk.step3;

import com.pszymczyk.ConsumerLoopManualCommit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Step3Runner {

    public static void main(String[] args) {
        int numConsumers = 3;
        String groupId = "step3";
        String topic = "step3";
        ExecutorService executor = Executors.newFixedThreadPool(3);
        final List<ConsumerLoopManualCommit> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoopManualCommit consumer = new ConsumerLoopManualCommit(i, groupId, topic);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (ConsumerLoopManualCommit consumer : consumers) {
                consumer.shutdown();
            }
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }
}
