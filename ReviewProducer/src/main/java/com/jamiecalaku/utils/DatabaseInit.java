package com.jamiecalaku.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DatabaseInit {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseInit.class);

    private static final String POSTGRES_URL = System.getenv("POSTGRES_URL");
    private static final String POSTGRES_USER = System.getenv("POSTGRES_USER");
    private static final String POSTGRES_PASSWORD = System.getenv("POSTGRES_PASSWORD");

    public static void run() {
        try (Connection conn = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
             Statement stmt = conn.createStatement()) {

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS reviews (
                        id INT PRIMARY KEY,
                        product VARCHAR(50) NOT NULL,
                        sentiment VARCHAR(20) NOT NULL,
                        body TEXT NOT NULL,
                        reviewer_reputation INT NOT NULL,
                        verified_purchase BOOLEAN NOT NULL,
                        timestamp TIMESTAMP NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS review_analysis (
                        id INT PRIMARY KEY,
                        product VARCHAR(50) NOT NULL,
                        sentiment_score INT NOT NULL,
                        body TEXT NOT NULL,
                        weight DOUBLE PRECISION NOT NULL,
                        timestamp TIMESTAMP NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS product_ranking (
                        product VARCHAR(50) NOT NULL,
                        total_reviews BIGINT NOT NULL,
                        sum_score INT NOT NULL,
                        sum_weight DOUBLE PRECISION NOT NULL,
                        average_score DOUBLE PRECISION NOT NULL,
                        smart_weighted_score DOUBLE PRECISION NOT NULL,
                        timestamp TIMESTAMP NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS product_alerts (
                        product VARCHAR(50) PRIMARY KEY,
                        previous_average DOUBLE PRECISION NOT NULL,
                        current_average DOUBLE PRECISION NOT NULL,
                        drop_percentage DOUBLE PRECISION NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        summary TEXT NOT NULL,
                        first_detected TIMESTAMP NOT NULL DEFAULT NOW(),
                        timestamp TIMESTAMP NOT NULL
                    );
                    TRUNCATE reviews, review_analysis, product_ranking, product_alerts;
                    """);

            logger.info("Database initialized and cleared\n");
        } catch (Exception e) {
            logger.error("Failed to initialize database", e);
            throw new RuntimeException(e);
        }
    }
}