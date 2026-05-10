package com.jamiecalaku;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.jamiecalaku.producer.ReviewProducer;

public class App
{
    public static void main( String[] args ) throws InterruptedException, JsonProcessingException {
        ReviewProducer.streamReviews();
    }
}
