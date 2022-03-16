package com.pszymczyk.step2;

import com.pszymczyk.ConsumerLoop;

import java.util.concurrent.Executors;

@SuppressWarnings("Duplicates")
public class Step2Runner {

    public static void main(String[] args) {
        var groupId = "step2";
        var topic = "step2";

        var executor = Executors.newFixedThreadPool(3);
        executor.submit(() -> {
            throw new RuntimeException("TODO");
        });
        executor.submit(() -> {
                throw new RuntimeException("TODO");
        });
        executor.submit(() -> {
            throw new RuntimeException("TODO");
        });
    }
}
