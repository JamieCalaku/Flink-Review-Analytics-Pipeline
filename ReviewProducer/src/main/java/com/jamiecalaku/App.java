package com.jamiecalaku;

import com.jamiecalaku.producer.ReviewProducer;
import com.jamiecalaku.utils.DatabaseInit;

public class App {
    public static void main(String[] args) {
        DatabaseInit.run();
        ReviewProducer.run();
    }
}
