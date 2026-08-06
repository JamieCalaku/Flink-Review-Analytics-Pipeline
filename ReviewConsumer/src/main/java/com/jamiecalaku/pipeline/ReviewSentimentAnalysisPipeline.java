package com.jamiecalaku.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jamiecalaku.llm.LLMClient;
import com.jamiecalaku.model.AnalysedReview;
import com.jamiecalaku.model.Review;
import com.jamiecalaku.utils.DatabaseConfig;
import com.jamiecalaku.utils.FileLoader;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

public class ReviewSentimentAnalysisPipeline {
    private static final Logger logger = LoggerFactory.getLogger(ReviewSentimentAnalysisPipeline.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SEPARATOR = "================================================================================";

    private static final int CONSUMER_ANALYSIS_DB_BATCH_SIZE = Integer.parseInt(System.getenv("CONSUMER_ANALYSIS_DB_BATCH_SIZE"));
    private static final long CONSUMER_ANALYSIS_DB_BATCH_INTERVAL = (long) (Double.parseDouble(System.getenv("CONSUMER_ANALYSIS_DB_BATCH_INTERVAL")) * 1000);

    private static final Duration CONSUMER_ANALYSIS_TIME_WINDOW = Duration.ofSeconds(Integer.parseInt(System.getenv("CONSUMER_ANALYSIS_TIME_WINDOW")));

    private static final double CONSUMER_ANALYSIS_VERIFIED_PURCHASE_MULTIPLIER = Double.parseDouble(System.getenv("CONSUMER_ANALYSIS_VERIFIED_PURCHASE_MULTIPLIER"));
    private static final double CONSUMER_ANALYSIS_VERIFIED_PURCHASE_DIVISOR = Double.parseDouble(System.getenv("CONSUMER_ANALYSIS_VERIFIED_PURCHASE_DIVISOR"));
    private static final double CONSUMER_ANALYSIS_REPUTATION_MULTIPLIER = Double.parseDouble(System.getenv("CONSUMER_ANALYSIS_REPUTATION_MULTIPLIER"));

    public static DataStream<AnalysedReview> build(DataStream<Review> reviewsStream) {
        // Collect reviews in time based windows and analyse them with the LLM in batches
        DataStream<AnalysedReview> analysedReviewsStream = reviewsStream
                .windowAll(TumblingProcessingTimeWindows.of(CONSUMER_ANALYSIS_TIME_WINDOW))
                .process(new ProcessAllWindowFunction<>() {
                    @Override
                    public void process(Context context, Iterable<Review> reviews, Collector<AnalysedReview> out) {
                        try {
                            List<Review> reviewsBatch = StreamSupport.stream(reviews.spliterator(), false).toList();
                            if (reviewsBatch.isEmpty()) return;

                            // Only send the id and body to the LLM to save tokens
                            String reviewsBatchPayload = objectMapper.writeValueAsString(
                                    reviewsBatch.stream()
                                            .map(review -> Map.of(
                                                    "id", (Object) review.getId(),
                                                    "body", (Object) review.getBody()
                                            ))
                                            .toList()
                            );

                            logger.info(SEPARATOR);
                            logger.info("Analysing batch of {} review(s)...", reviewsBatch.size());
                            logger.info(SEPARATOR + "\n");

                            List<AnalysedReview> analysedReviews = analyseReviews(reviewsBatchPayload);

                            logger.info("Analysed batch of {} review(s)...\n", analysedReviews.size());

                            // Enrich each analysed review with data from the original review
                            for (AnalysedReview analysedReview : analysedReviews) {
                                Review originalReview = reviewsBatch.stream()
                                        .filter(review -> review.getId() == analysedReview.getId())
                                        .findFirst().orElse(null);

                                if (originalReview != null) {
                                    analysedReview.setBody(originalReview.getBody());
                                    analysedReview.setTimestamp(originalReview.getTimestamp());
                                }

                                analysedReview.setWeight(calculateWeight(originalReview));

                                logger.debug("{}\n", objectMapper.writeValueAsString(analysedReview));

                                out.collect(analysedReview);
                            }
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to process review batch", e);
                        }
                    }
                });

        analysedReviewsStream.addSink(
                JdbcSink.sink(
                        """
                                INSERT INTO review_analysis
                                    (id, product, sentiment_score, body, weight, timestamp)
                                VALUES (?, ?, ?, ?, ?, ?)
                                ON CONFLICT (id)
                                DO UPDATE SET
                                    product = EXCLUDED.product,
                                    sentiment_score = EXCLUDED.sentiment_score,
                                    body = EXCLUDED.body,
                                    weight = EXCLUDED.weight,
                                    timestamp = EXCLUDED.timestamp;
                            """,
                        (statement, analysedReview) -> {
                            statement.setInt(1, analysedReview.getId());
                            statement.setString(2, analysedReview.getProduct().name());
                            statement.setInt(3, analysedReview.getSentimentScore());
                            statement.setString(4, analysedReview.getBody());
                            statement.setDouble(5, analysedReview.getWeight());
                            statement.setTimestamp(6, analysedReview.getTimestamp());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(CONSUMER_ANALYSIS_DB_BATCH_SIZE)
                                .withBatchIntervalMs(CONSUMER_ANALYSIS_DB_BATCH_INTERVAL)
                                .build(),
                        DatabaseConfig.get()
                )
        );
        return analysedReviewsStream;
    }

    private static List<AnalysedReview> analyseReviews(String reviewsJson) throws JsonProcessingException {
        String prompt = preparePrompt(reviewsJson);

        return invokeSentimentAnalysis(prompt);
    }

    private static String preparePrompt(String reviewsJson) {
        String prompt = FileLoader.getReviewAnalysisPrompt();
        prompt = prompt.replace("{REVIEWS}", reviewsJson);

        return prompt;
    }

    private static List<AnalysedReview> invokeSentimentAnalysis(String prompt) throws JsonProcessingException {
        String modelOutput = LLMClient.invokeModel(prompt, FileLoader.getReviewAnalysisSchema());

        String analysedReviewsJson = objectMapper.readTree(modelOutput)
                .path("content").get(0)
                .path("text").asText();

        return objectMapper.readValue(analysedReviewsJson, new TypeReference<>() {});
    }

    private static double calculateWeight(Review originalReview) {
        if (originalReview == null) return 1.0;

        // Verified purchases are weighted 1.5x, not verified purchases are weighted 0.5x and each reputation point adds 0.15x | these are the default values
        double verifiedMultiplier = originalReview.isVerifiedPurchase() ? CONSUMER_ANALYSIS_VERIFIED_PURCHASE_MULTIPLIER : CONSUMER_ANALYSIS_VERIFIED_PURCHASE_DIVISOR;
        int reviewerReputation = originalReview.getReviewerReputation();

        return Math.round(verifiedMultiplier * (1.0 + (reviewerReputation * CONSUMER_ANALYSIS_REPUTATION_MULTIPLIER)) * 100.0) / 100.0;
    }
}
