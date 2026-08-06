package com.jamiecalaku.model;

import java.sql.Timestamp;

public class AnalysedReview {
    private int id;
    private Product product;
    private int sentimentScore;
    private String body;
    private double weight;
    private Timestamp timestamp;

    public AnalysedReview() {
    }

    public AnalysedReview(int id, Product product, int sentimentScore, String body, double weight, Timestamp timestamp) {
        this.id = id;
        this.product = product;
        this.sentimentScore = sentimentScore;
        this.body = body;
        this.weight = weight;
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

    public int getSentimentScore() {
        return sentimentScore;
    }

    public void setSentimentScore(int sentimentScore) {
        this.sentimentScore = sentimentScore;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}
