package com.pszymczyk.step2;

import com.pszymczyk.ConsumerLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

@SuppressWarnings("Duplicates")
public class Step2Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step2Runner.class);

    public static void main(String[] args) {
        var groupId = "step2";
        var topic = "step2";

        var executor = Executors.newFixedThreadPool(3);
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoop(0, groupId, topic)) {
                consumerLoop.start();
            }
        });
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoop(1, groupId, topic)) {
                consumerLoop.start();
            }
        });
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoop(2, groupId, topic)) {
                consumerLoop.start();
            }
        });
    }
}
