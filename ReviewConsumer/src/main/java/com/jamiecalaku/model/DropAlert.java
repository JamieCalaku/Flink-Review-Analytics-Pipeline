package com.jamiecalaku.model;

import java.sql.Timestamp;

public class DropAlert {
    private Product product;
    private double previousAverage;
    private double currentAverage;
    private double dropPercentage;
    private AlertSeverity severity;
    private String summary;
    private Timestamp timestamp;

    public DropAlert() {
    }

    public DropAlert(Product product, double previousAverage, double currentAverage, double dropPercentage, AlertSeverity severity, String summary, Timestamp timestamp) {
        this.product = product;
        this.previousAverage = previousAverage;
        this.currentAverage = currentAverage;
        this.dropPercentage = dropPercentage;
        this.severity = severity;
        this.summary = summary;
        this.timestamp = timestamp;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }

    public double getPreviousAverage() {
        return previousAverage;
    }

    public void setPreviousAverage(double previousAverage) {
        this.previousAverage = previousAverage;
    }

    public double getCurrentAverage() {
        return currentAverage;
    }

    public void setCurrentAverage(double currentAverage) {
        this.currentAverage = currentAverage;
    }

    public double getDropPercentage() {
        return dropPercentage;
    }

    public void setDropPercentage(double dropPercentage) {
        this.dropPercentage = dropPercentage;
    }

    public AlertSeverity getSeverity() {
        return severity;
    }

    public void setSeverity(AlertSeverity severity) {
        this.severity = severity;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}
