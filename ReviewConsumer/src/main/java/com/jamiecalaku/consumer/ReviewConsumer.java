package com.jamiecalaku.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jamiecalaku.model.AnalysedReview;
import com.jamiecalaku.model.Review;
import com.jamiecalaku.pipeline.ProductDropDetectionPipeline;
import com.jamiecalaku.pipeline.ProductRankingPipeline;
import com.jamiecalaku.pipeline.ReviewIngestionPipeline;
import com.jamiecalaku.pipeline.ReviewSentimentAnalysisPipeline;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReviewConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ReviewConsumer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final int CONSUMER_FLINK_UI_PORT = Integer.parseInt(System.getenv("CONSUMER_FLINK_UI_PORT"));

    private static final StreamExecutionEnvironment env = createEnvironment();

    private static final String SEPARATOR = "================================================================================";

    private static final String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");
    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS");

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...\n");
        }));
    }

    // Flink Dashboard Configuration
    private static StreamExecutionEnvironment createEnvironment() {
        Configuration config = new Configuration();
        config.set(RestOptions.BIND_ADDRESS, "0.0.0.0");
        config.set(RestOptions.PORT, CONSUMER_FLINK_UI_PORT);

        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
    }

    public static void run() throws Exception {
        logger.info(SEPARATOR);
        logger.info("Starting...");
        logger.info(SEPARATOR + "\n");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_TOPIC)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> reviewsJson = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "reviews");
        DataStream<Review> reviewsStream = reviewsJson.map(json -> objectMapper.readValue(json, Review.class));

        // This pipeline saves the reviews unchanged to the database
        ReviewIngestionPipeline.build(reviewsStream);

        // This pipeline analyses the reviews with an LLM
        // The LLM returns a sentiment score and finds out which product the review is about
        DataStream<AnalysedReview> analysedReviewsStream = ReviewSentimentAnalysisPipeline.build(reviewsStream);

        // This pipeline ranks the products based on the analysed reviews
        ProductRankingPipeline.build(analysedReviewsStream);

        // This pipeline recognizes when a products average sentiment drops
        ProductDropDetectionPipeline.build(analysedReviewsStream);

        env.execute();
    }
}