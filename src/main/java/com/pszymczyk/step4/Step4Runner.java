package com.pszymczyk.step4;

import com.pszymczyk.ConsumerLoop;
import com.pszymczyk.ConsumerLoopWithCutomDeserializer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Step4Runner {

    public static void main(String[] args) {
        String groupId = "step4";
        String topic = "step4";
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ConsumerLoopWithCutomDeserializer consumer = new ConsumerLoopWithCutomDeserializer(0, groupId, topic);
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
