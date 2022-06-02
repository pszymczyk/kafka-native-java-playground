package com.pszymczyk;

import java.util.Random;

public class Utils {
    
    public static void failSometimes() {
        Random rand = new Random();
        int randomNum = rand.nextInt((3 - 1) + 1) + 1;
        if (randomNum == 2) {
            throw new RuntimeException("Random number 2 = exception!");
        }
    }
}
