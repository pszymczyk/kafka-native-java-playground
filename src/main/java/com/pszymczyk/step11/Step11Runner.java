package com.pszymczyk.step11;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Step11Runner {

    protected static Logger logger = LoggerFactory.getLogger(Step11Runner.class);

    public static void main(String[] args) throws InterruptedException {
        String inputTopic = "step11-input";
        String outputTopic = "step11-output";
        String groupId = "step11";

        ExecutorService executor = Executors.newSingleThreadExecutor();
        FailingSplitter failingSplitter = new FailingSplitter(inputTopic, outputTopic, groupId);
        executor.submit(failingSplitter);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            failingSplitter.shutdown();
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

}
