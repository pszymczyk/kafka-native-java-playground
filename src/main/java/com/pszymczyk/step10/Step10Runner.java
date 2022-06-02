package com.pszymczyk.step10;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Step10Runner {

    private static final Logger logger = LoggerFactory.getLogger(Step10Runner.class);

    public static void main(String[] args) {
        String inputTopic = "loan-application-requests";
        String outputTopic = "loan-application-decisions";
        String groupId = "step10";

        try (var loanApplicationProcess = new LoanApplicationProcess(inputTopic, outputTopic, groupId, new DebtorsRepository() { })) {
            loanApplicationProcess.start();
        }
    }
}
