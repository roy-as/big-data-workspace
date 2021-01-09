package com.as;

import java.math.BigDecimal;

public class Pi {

    public static void main(String[] args) {
        int sum = 100000000;
        int cicle = 0;
        for(int i = 0; i < sum; i++) {
            double x = Math.random();
            double y = Math.random();
            BigDecimal multiply1 = new BigDecimal(x).multiply(new BigDecimal(x));
            BigDecimal multiply2 = new BigDecimal(y).multiply(new BigDecimal(y));
            int result = multiply1.add(multiply2).compareTo(new BigDecimal(1));
            if(result <= 0) {
                cicle++;
            }

        }
        double pi = 4.0 * cicle / sum;
        System.out.println(pi);
    }
}
