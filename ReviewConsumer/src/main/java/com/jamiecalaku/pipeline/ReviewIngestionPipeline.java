package com.jamiecalaku.pipeline;

import com.jamiecalaku.model.Review;
import com.jamiecalaku.utils.DatabaseConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

public class ReviewIngestionPipeline {
    private static final int CONSUMER_INGESTION_DB_BATCH_SIZE = Integer.parseInt(System.getenv("CONSUMER_INGESTION_DB_BATCH_SIZE"));
    private static final long CONSUMER_INGESTION_DB_BATCH_INTERVAL = (long) (Double.parseDouble(System.getenv("CONSUMER_INGESTION_DB_BATCH_INTERVAL")) * 1000);

    public static void build(DataStream<Review> reviewsStream) {
        reviewsStream.addSink(
                JdbcSink.sink(
                    """
                            INSERT INTO reviews
                                (id, product, sentiment, body, reviewer_reputation, verified_purchase, timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (id)
                            DO UPDATE SET
                                product = EXCLUDED.product,
                                sentiment = EXCLUDED.sentiment,
                                body = EXCLUDED.body,
                                reviewer_reputation = EXCLUDED.reviewer_reputation,
                                verified_purchase = EXCLUDED.verified_purchase,
                                timestamp = EXCLUDED.timestamp;
                        """,
                        (statement, review) -> {
                            statement.setInt(1, review.getId());
                            statement.setString(2, review.getProduct().name());
                            statement.setString(3, review.getSentiment().name());
                            statement.setString(4, review.getBody());
                            statement.setInt(5, review.getReviewerReputation());
                            statement.setBoolean(6, review.isVerifiedPurchase());
                            statement.setTimestamp(7, review.getTimestamp());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(CONSUMER_INGESTION_DB_BATCH_SIZE)
                                .withBatchIntervalMs(CONSUMER_INGESTION_DB_BATCH_INTERVAL)
                                .build(),
                        DatabaseConfig.get()
                )
        );
    }
}
