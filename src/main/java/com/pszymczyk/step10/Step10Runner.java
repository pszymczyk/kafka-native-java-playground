package com.pszymczyk.step10;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Step10Runner {

    protected static Logger logger = LoggerFactory.getLogger(Step10Runner.class);

    public static void main(String[] args) throws InterruptedException {
        String inputTopic = "loan-application-requests";
        String outputTopic = "loan-application-decisions";
        String groupId = "step10";

        ExecutorService executor = Executors.newSingleThreadExecutor();
        LoanApplicationProcess loanApplicationProcess = new LoanApplicationProcess(inputTopic, outputTopic, groupId, new DebtorsRepository() { });
        executor.submit(loanApplicationProcess);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            loanApplicationProcess.shutdown();
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

}
