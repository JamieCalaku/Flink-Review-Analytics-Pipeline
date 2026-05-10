package com.jamiecalaku.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class SentimentAssignment {

    private static final Logger logger = LoggerFactory.getLogger(SentimentAssignment.class);

    private static final String iphoneSentiment;
    private static final String macbookSentiment;
    private static final String airpodsSentiment;
    private static final String ipadSentiment;

    static {
        List<String> roles = new ArrayList<>(List.of(
                "POSITIVE_HEAVY",
                "POSITIVE",
                "NEUTRAL",
                "NEGATIVE"
        ));
        Collections.shuffle(roles, new Random());

        iphoneSentiment = roles.get(0);
        macbookSentiment = roles.get(1);
        airpodsSentiment = roles.get(2);
        ipadSentiment = roles.get(3);

        logger.info("Sentiment Assignment: \n"
                + " IphoneSentiment: " + iphoneSentiment + "\n"
                + " MacbookSentiment: " + macbookSentiment + "\n"
                + " AirpodsSentiment: " + airpodsSentiment + "\n"
                + " IpadSentiment: " + ipadSentiment + "\n"
        );
    }

    public static String getDropSentiment(String sentiment) {
        return "POSITIVE".equals(sentiment) ? "DROP" : sentiment;
    }


    public static String getIphoneSentiment() {
        return iphoneSentiment;
    }

    public static String getMacbookSentiment() {
        return macbookSentiment;
    }

    public static String getAirpodsSentiment() {
        return airpodsSentiment;
    }

    public static String getIpadSentiment() {
        return ipadSentiment;
    }
}