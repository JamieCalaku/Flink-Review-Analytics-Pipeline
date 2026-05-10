package com.jamiecalaku.model;

public class Review {
    private Integer id;
    private String product;
    private String sentiment;
    private String body;
    private Integer helpfulVotes;
    private Boolean verifiedPurchase;
    private Long timestamp;

    public Review() {}

    public Review(Integer id, String product, String sentiment, String body, Integer helpfulVotes, Boolean verifiedPurchase, Long timestamp) {
        this.id = id;
        this.product = product;
        this.sentiment = sentiment;
        this.body = body;
        this.helpfulVotes = helpfulVotes;
        this.verifiedPurchase = verifiedPurchase;
        this.timestamp = timestamp;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getSentiment() {
        return sentiment;
    }

    public void setSentiment(String sentiment) {
        this.sentiment = sentiment;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public Integer getHelpfulVotes() {
        return helpfulVotes;
    }

    public void setHelpfulVotes(Integer helpfulVotes) {
        this.helpfulVotes = helpfulVotes;
    }

    public Boolean getVerifiedPurchase() {
        return verifiedPurchase;
    }

    public void setVerifiedPurchase(Boolean verifiedPurchase) {
        this.verifiedPurchase = verifiedPurchase;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
