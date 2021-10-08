package com.pszymczyk.step1;

import com.pszymczyk.ConsumerLoop;

public class Step1Runner {

    public static void main(String[] args) {
        var groupId = "step1";
        var topic = "step1";
        try (var consumer = new ConsumerLoop( groupId, topic)) {
            consumer.start();
        }
    }
}
