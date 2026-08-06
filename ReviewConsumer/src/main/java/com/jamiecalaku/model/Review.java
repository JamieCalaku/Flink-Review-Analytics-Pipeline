package com.jamiecalaku.model;

import java.sql.Timestamp;

public class Review {
    private int id;
    private Product product;
    private Sentiment sentiment;
    private String body;
    private int reviewerReputation;
    private boolean verifiedPurchase;
    private Timestamp timestamp;

    public Review() {}

    public Review(int id, Product product, Sentiment sentiment, String body, int reviewerReputation, boolean verifiedPurchase, Timestamp timestamp) {
        this.id = id;
        this.product = product;
        this.sentiment = sentiment;
        this.body = body;
        this.reviewerReputation = reviewerReputation;
        this.verifiedPurchase = verifiedPurchase;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }

    public Sentiment getSentiment() {
        return sentiment;
    }

    public void setSentiment(Sentiment sentiment) {
        this.sentiment = sentiment;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public int getReviewerReputation() {
        return reviewerReputation;
    }

    public void setReviewerReputation(int reviewerReputation) {
        this.reviewerReputation = reviewerReputation;
    }

    public boolean isVerifiedPurchase() {
        return verifiedPurchase;
    }

    public void setVerifiedPurchase(boolean verifiedPurchase) {
        this.verifiedPurchase = verifiedPurchase;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}
