package com.pszymczyk;

import com.pszymczyk.step2.Step2Runner;
import com.pszymczyk.step3.ConsumerLoopManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static void failSometimes() {
        Random rand = new Random();
        int randomNum = rand.nextInt((3 - 1) + 1) + 1;
        if (randomNum == 2) {
            throw new RuntimeException("Random number 2 = exception!");
        }
    }

    public static void wakeUpConsumer(String consumerName, ConsumerLoop consumerLoop, Thread thread) {
        logger.info("Hello consumer {}, wakeup!", consumerName);
        consumerLoop.wakeup();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void wakeUpConsumer(String consumerName, ConsumerLoopManualCommit consumerLoop, Thread thread) {
        logger.info("Hello consumer {}, wakeup!", consumerName);
        consumerLoop.wakeup();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
