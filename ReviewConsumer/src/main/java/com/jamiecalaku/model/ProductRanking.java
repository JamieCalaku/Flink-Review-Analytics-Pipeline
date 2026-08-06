package com.jamiecalaku.model;

import java.sql.Timestamp;

public class ProductRanking {
    private Product product;
    private long totalReviews;
    private int sumScore;
    private double sumWeight;
    private double averageScore;
    private double smartWeightedScore;
    private Timestamp timestamp;

    public ProductRanking() {
    }

    public ProductRanking(Product product, long totalReviews, int sumScore, double sumWeight, double averageScore, double smartWeightedScore, Timestamp timestamp) {
        this.product = product;
        this.totalReviews = totalReviews;
        this.sumScore = sumScore;
        this.sumWeight = sumWeight;
        this.averageScore = averageScore;
        this.smartWeightedScore = smartWeightedScore;
        this.timestamp = timestamp;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }

    public long getTotalReviews() {
        return totalReviews;
    }

    public void setTotalReviews(long totalReviews) {
        this.totalReviews = totalReviews;
    }

    public int getSumScore() {
        return sumScore;
    }

    public void setSumScore(int sumScore) {
        this.sumScore = sumScore;
    }

    public double getSumWeight() {
        return sumWeight;
    }

    public void setSumWeight(double sumWeight) {
        this.sumWeight = sumWeight;
    }

    public double getAverageScore() {
        return averageScore;
    }

    public void setAverageScore(double averageScore) {
        this.averageScore = averageScore;
    }

    public double getSmartWeightedScore() {
        return smartWeightedScore;
    }

    public void setSmartWeightedScore(double smartWeightedScore) {
        this.smartWeightedScore = smartWeightedScore;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}
