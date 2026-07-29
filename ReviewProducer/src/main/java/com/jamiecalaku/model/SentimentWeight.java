package com.jamiecalaku.model;

public enum SentimentWeight {
    POSITIVE_HEAVY,
    POSITIVE,
    NEUTRAL,
    NEGATIVE,
    DROP;

    public final int positiveWeight;
    public final int neutralWeight;
    public final int negativeWeight;

    SentimentWeight() {
        // Load weights from env
        String[] parts = System.getenv("PRODUCER_WEIGHT_" + this.name()).split(",");
        this.positiveWeight = Integer.parseInt(parts[0]);
        this.neutralWeight  = Integer.parseInt(parts[1]);
        this.negativeWeight = Integer.parseInt(parts[2]);
    }
}