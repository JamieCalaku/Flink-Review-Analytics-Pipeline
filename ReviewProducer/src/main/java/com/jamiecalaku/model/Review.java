package com.jamiecalaku.model;

public class Review {
    private int id;
    private Product product;
    private Sentiment sentiment;
    private String body;
    private int helpfulVotes;
    private boolean verifiedPurchase;
    private long timestamp;

    public Review() {}

    public Review(int id, Product product, Sentiment sentiment, String body, int helpfulVotes, boolean verifiedPurchase, long timestamp) {
        this.id = id;
        this.product = product;
        this.sentiment = sentiment;
        this.body = body;
        this.helpfulVotes = helpfulVotes;
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

    public int getHelpfulVotes() {
        return helpfulVotes;
    }

    public void setHelpfulVotes(int helpfulVotes) {
        this.helpfulVotes = helpfulVotes;
    }

    public boolean isVerifiedPurchase() {
        return verifiedPurchase;
    }

    public void setVerifiedPurchase(boolean verifiedPurchase) {
        this.verifiedPurchase = verifiedPurchase;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
