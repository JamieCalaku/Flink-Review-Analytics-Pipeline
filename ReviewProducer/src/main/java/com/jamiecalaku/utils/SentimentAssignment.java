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

    private static final SentimentWeight gta6Sentiment;
    private static final SentimentWeight fc26Sentiment;
    private static final SentimentWeight callOfDutySentiment;
    private static final SentimentWeight forzaHorizon6Sentiment;

    static {
        List<SentimentWeight> sentiments = new ArrayList<>(List.of(
                SentimentWeight.POSITIVE_HEAVY,
                SentimentWeight.POSITIVE,
                SentimentWeight.NEUTRAL,
                SentimentWeight.NEGATIVE
        ));
        // Assign sentiments to products randomly by shuffling
        Collections.shuffle(sentiments, ThreadLocalRandom.current());

        gta6Sentiment = sentiments.get(0);
        fc26Sentiment = sentiments.get(1);
        callOfDutySentiment = sentiments.get(2);
        forzaHorizon6Sentiment = sentiments.get(3);

        logger.info("""

             GTA 6 Sentiment:           {}
             FC 26 Sentiment:           {}
             Call of Duty Sentiment:    {}
             Forza Horizon 6 Sentiment: {}
             """, gta6Sentiment, fc26Sentiment, callOfDutySentiment, forzaHorizon6Sentiment
        );
    }

    public static Map<Sentiment, Integer> distributeReviews(SentimentWeight sentimentWeight, int totalReviews) {
        // Convert Percentages to actual numbers and slightly randomize it
        int positive = applyWeight(totalReviews, sentimentWeight.positiveWeight);
        int neutral  = applyWeight(totalReviews, sentimentWeight.neutralWeight);
        int negative = Math.max(0, totalReviews - positive - neutral);

        return Map.of(
                Sentiment.POSITIVE, positive,
                Sentiment.NEUTRAL, neutral,
                Sentiment.NEGATIVE, negative
        );
    }

    private static int applyWeight(int totalReviews, int weightPercentage) {
        // Calculate variation based on total reviews
        int base = Math.round(totalReviews * weightPercentage / 100f);
        int maxVariation = Math.max(1, totalReviews / 20);
        int variation = ThreadLocalRandom.current().nextInt(-maxVariation, maxVariation + 1);
        return Math.max(0, base + variation);
    }

    // This method will return DROP when the sentiment equals exactly positive
    // This is used to initiate the sentiment drop for the product with the positive sentiment
    public static SentimentWeight getDropSentiment(SentimentWeight sentiment) {
        return sentiment == SentimentWeight.POSITIVE ? SentimentWeight.DROP : sentiment;
    }

    public static String getDroppedProduct() {
        if (gta6Sentiment == SentimentWeight.POSITIVE) return "GTA_6";
        if (fc26Sentiment == SentimentWeight.POSITIVE) return "FC_26";
        if (callOfDutySentiment == SentimentWeight.POSITIVE) return "CALL_OF_DUTY";
        if (forzaHorizon6Sentiment == SentimentWeight.POSITIVE) return "FORZA_HORIZON_6";
        return "UNKNOWN";
    }

    public static SentimentWeight getGta6Sentiment() {
        return gta6Sentiment;
    }

    public static SentimentWeight getFc26Sentiment() {
        return fc26Sentiment;
    }

    public static SentimentWeight getCallOfDutySentiment() {
        return callOfDutySentiment;
    }

    public static SentimentWeight getForzaHorizon6Sentiment() {
        return forzaHorizon6Sentiment;
    }

}