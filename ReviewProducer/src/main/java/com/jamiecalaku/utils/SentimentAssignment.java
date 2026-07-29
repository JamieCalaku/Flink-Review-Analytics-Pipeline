package com.jamiecalaku.utils;

import com.jamiecalaku.model.SentimentWeight;
import com.jamiecalaku.model.Sentiment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SentimentAssignment {
    private static final Logger logger = LoggerFactory.getLogger(SentimentAssignment.class);

    private static final SentimentWeight iphoneSentiment;
    private static final SentimentWeight macbookSentiment;
    private static final SentimentWeight airpodsSentiment;
    private static final SentimentWeight ipadSentiment;

    static {
        List<SentimentWeight> sentiments = new ArrayList<>(List.of(
                SentimentWeight.POSITIVE_HEAVY,
                SentimentWeight.POSITIVE,
                SentimentWeight.NEUTRAL,
                SentimentWeight.NEGATIVE
        ));
        // Assign sentiments to products randomly by shuffling
        Collections.shuffle(sentiments, ThreadLocalRandom.current());

        iphoneSentiment = sentiments.get(0);
        macbookSentiment = sentiments.get(1);
        airpodsSentiment = sentiments.get(2);
        ipadSentiment = sentiments.get(3);

        logger.info("""
             \nIphone Sentiment:  {}
             Macbook Sentiment: {}
             Airpods Sentiment: {}
             Ipad Sentiment:    {}
             """, iphoneSentiment, macbookSentiment, airpodsSentiment, ipadSentiment);
    }

    public static Map<Sentiment, Integer> distributeReviews(SentimentWeight sentimentWeight, int totalReviews) {
        // Convert Percentages to actual numbers and slightly randomize it
        int positive = applyWeight(totalReviews, sentimentWeight.positiveWeight);
        int neutral  = applyWeight(totalReviews, sentimentWeight.neutralWeight);

        // Remaining will be negative
        int negative = totalReviews - positive - neutral;

        // If negative is less than 0, we need to adjust positive and negative counts to ensure they sum up to totalReviews
        if (negative < 0) {
            positive += negative;
            negative = 0;
        }

        return Map.of(
                Sentiment.POSITIVE, positive,
                Sentiment.NEUTRAL, neutral,
                Sentiment.NEGATIVE, negative
        );
    }

    private static int applyWeight(int totalReviews, int weightPercentage) {
        int base = Math.round(totalReviews * weightPercentage / 100f);
        int maxVariation = Math.max(1, totalReviews / 20);
        int variation = ThreadLocalRandom.current().nextInt(-maxVariation, maxVariation + 1);
        return Math.max(0, base + variation);
    }

    // This method will return DROP when the sentiment equals exactly positive
    public static SentimentWeight getDropSentiment(SentimentWeight sentiment) {
        return sentiment == SentimentWeight.POSITIVE ? SentimentWeight.DROP : sentiment;
    }

    public static SentimentWeight getIphoneSentiment() {
        return iphoneSentiment;
    }

    public static SentimentWeight getMacbookSentiment() {
        return macbookSentiment;
    }

    public static SentimentWeight getAirpodsSentiment() {
        return airpodsSentiment;
    }

    public static SentimentWeight getIpadSentiment() {
        return ipadSentiment;
    }
}