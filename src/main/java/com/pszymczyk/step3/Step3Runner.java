package com.pszymczyk.step3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

@SuppressWarnings("Duplicates")
public class Step3Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step3Runner.class);

    public static void main(String[] args) {
        var groupId = "step3";
        var topic = "step3";
        var executor = Executors.newFixedThreadPool(3);
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoopManualCommit(0, groupId, topic)) {
                consumerLoop.start();
            }
        });
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoopManualCommit(1, groupId, topic)) {
                consumerLoop.start();
            }
        });
        executor.submit(() -> {
            try (var consumerLoop = new ConsumerLoopManualCommit(2, groupId, topic)) {
                consumerLoop.start();
            }
        });
    }
}
