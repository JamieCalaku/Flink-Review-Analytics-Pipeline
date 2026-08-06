package com.jamiecalaku.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jamiecalaku.llm.LLMClient;
import com.jamiecalaku.model.SentimentWeight;
import com.jamiecalaku.model.Review;
import com.jamiecalaku.model.Sentiment;
import com.jamiecalaku.utils.FileLoader;
import com.jamiecalaku.utils.SentimentAssignment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;

public class ReviewProducer {
    private static final Logger logger = LoggerFactory.getLogger(ReviewProducer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final KafkaProducer<String, String> kafkaProducer;

    private static final String SEPARATOR = "================================================================================";

    private static final String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");
    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS");

    private static final int BASELINE_REVIEW_COUNT = Integer.parseInt(System.getenv("PRODUCER_BASELINE_REVIEW_COUNT"));
    private static final int DROP_REVIEW_COUNT = Integer.parseInt(System.getenv("PRODUCER_DROP_REVIEW_COUNT"));

    private static final long SLEEP_START = (long) (Double.parseDouble(System.getenv("PRODUCER_SLEEP_START")) * 1000);
    private static final long SLEEP_END = (long) (Double.parseDouble(System.getenv("PRODUCER_SLEEP_END")) * 1000);

    private static final int MINORITY_REPUTATION_MIN = Integer.parseInt(System.getenv("PRODUCER_MINORITY_REPUTATION_MIN"));
    private static final int MINORITY_REPUTATION_MAX = Integer.parseInt(System.getenv("PRODUCER_MINORITY_REPUTATION_MAX"));
    private static final int MAJORITY_REPUTATION_MIN = Integer.parseInt(System.getenv("PRODUCER_MAJORITY_REPUTATION_MIN"));
    private static final int MAJORITY_REPUTATION_MAX = Integer.parseInt(System.getenv("PRODUCER_MAJORITY_REPUTATION_MAX"));

    private static final double MINORITY_VERIFIED_CHANCE = Double.parseDouble(System.getenv("PRODUCER_MINORITY_VERIFIED_CHANCE"));
    private static final double MAJORITY_VERIFIED_CHANCE = Double.parseDouble(System.getenv("PRODUCER_MAJORITY_VERIFIED_CHANCE"));

    private enum Stage {BASELINE, SENTIMENT_DROP}

    private static int reviewIdCounter = 1;

    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<>(properties);

        // Create Kafka Topic
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(List.of(new NewTopic(KAFKA_TOPIC, 1, (short) 1)));
        } catch (Exception e) {
            logger.debug("Kafka topic {} already exists", KAFKA_TOPIC);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down - flushing Kafka producer...\n");
            kafkaProducer.flush();
            kafkaProducer.close();
        }));
    }

    public static void run() {
        logger.info(SEPARATOR);
        logger.info("Starting...");
        logger.info(SEPARATOR + "\n");

        long producerStartTime = System.currentTimeMillis();

        // Generate baseline reviews
        List<Review> baselineReviews;
        try {
            baselineReviews = generateReviews(Stage.BASELINE);
        } catch (JsonProcessingException e) {
            logger.error(SEPARATOR);
            logger.error("(BASELINE) Failed to parse baseline reviews", e);
            logger.error(SEPARATOR);

            throw new RuntimeException();
        }

        // Generate sentiment drop reviews asynchronously so the baseline reviews can be published
        List<Review> sentimentDropReviews = List.of();
        CompletableFuture<List<Review>> sentimentDropReviewsFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return generateReviews(Stage.SENTIMENT_DROP);
            } catch (JsonProcessingException e) {
                logger.error(SEPARATOR);
                logger.error("(SENTIMENT_DROP) Failed to parse sentiment drop reviews", e);
                logger.error(SEPARATOR);

                throw new RuntimeException();
            }
        });

        try {
            // Baseline Stage
            logger.info(SEPARATOR);
            logger.info("(BASELINE) Publishing {} reviews -> Kafka...", baselineReviews.size());
            logger.info(SEPARATOR + "\n");
            publishToKafka(baselineReviews);

            // Sentiment Drop Stage
            sentimentDropReviews = sentimentDropReviewsFuture.join();

            logger.info(SEPARATOR);
            logger.info("(SENTIMENT_DROP) Publishing {} reviews -> Kafka...", sentimentDropReviews.size());
            logger.info(SEPARATOR + "\n");
            publishToKafka(sentimentDropReviews);
        } catch (InterruptedException | CompletionException e) {
            logger.error(SEPARATOR);
            logger.error("There was an unexpected error when trying to publish a review", e);
            logger.error(SEPARATOR);
        } finally {
            long producerDuration = System.currentTimeMillis() - producerStartTime;

            logger.info(SEPARATOR);
            logger.info("finished producing - total reviews: {} - duration: {}s", baselineReviews.size() + sentimentDropReviews.size(), producerDuration / 1000);
            logger.info(SEPARATOR + "\n");
        }
    }

    private static List<Review> generateReviews(Stage stage) throws JsonProcessingException {
        Map<String, SentimentWeight> productAssignedSentiments = getProductAssignedSentiments(stage);
        String prompt = preparePrompt(stage, productAssignedSentiments);
        List<Review> reviews = invokeReviewGeneration(prompt);

        // Shuffle all the reviews for more realistic data and assign them an id and weights
        Collections.shuffle(reviews, ThreadLocalRandom.current());
        for (Review review : reviews) {
            review.setId(reviewIdCounter++);
            applyReviewMetadata(review, productAssignedSentiments);
        }

        return reviews;
    }

    private static Map<String, SentimentWeight> getProductAssignedSentiments(Stage stage) {
        if (stage == Stage.BASELINE) {
            return Map.of(
                    "GTA_6", SentimentAssignment.getGta6Sentiment(),
                    "FC_26", SentimentAssignment.getFc26Sentiment(),
                    "CALL_OF_DUTY", SentimentAssignment.getCallOfDutySentiment(),
                    "FORZA_HORIZON_6", SentimentAssignment.getForzaHorizon6Sentiment()
            );
        }

        return Map.of(
                "GTA_6", SentimentAssignment.getDropSentiment(SentimentAssignment.getGta6Sentiment()),
                "FC_26", SentimentAssignment.getDropSentiment(SentimentAssignment.getFc26Sentiment()),
                "CALL_OF_DUTY", SentimentAssignment.getDropSentiment(SentimentAssignment.getCallOfDutySentiment()),
                "FORZA_HORIZON_6", SentimentAssignment.getDropSentiment(SentimentAssignment.getForzaHorizon6Sentiment())
        );
    }

    private static String preparePrompt(Stage stage, Map<String, SentimentWeight> productAssignedSentiments) {
        String prompt;
        int reviewCount;

        if (stage == Stage.BASELINE) {
            prompt = FileLoader.getReviewGenerationPrompt();
            reviewCount = BASELINE_REVIEW_COUNT;
        } else {
            prompt = FileLoader.getReviewDropGenerationPrompt();
            reviewCount = DROP_REVIEW_COUNT;
            prompt = prompt.replace("{DROPPED_PRODUCT}", SentimentAssignment.getDroppedProduct());
        }

        int reviewsPerProduct = reviewCount / productAssignedSentiments.size();

        // We give the llm explicit id ranges for each product and all sentiments to ensure that the llm follows the distribution we want
        StringBuilder distribution = new StringBuilder();
        int nextId = 1;

        for (Map.Entry<String, SentimentWeight> entry : productAssignedSentiments.entrySet()) {
            String product = entry.getKey();
            Map<Sentiment, Integer> counts = SentimentAssignment.distributeReviews(entry.getValue(), reviewsPerProduct);

            for (Sentiment sentiment : Sentiment.values()) {
                int count = counts.get(sentiment);
                if (count == 0) continue;

                distribution.append("  - %s %s: ids %d-%d%n"
                        .formatted(product, sentiment, nextId, nextId + count - 1));
                nextId += count;
            }
        }

        prompt = prompt.replace("{REVIEW_COUNT}", String.valueOf(nextId - 1));
        prompt = prompt.replace("{DISTRIBUTION}", distribution.toString());

        return prompt;
    }

    private static List<Review> invokeReviewGeneration(String prompt) throws JsonProcessingException {
        String modelOutput = LLMClient.invokeModel(prompt, FileLoader.getReviewGenerationSchema());
        String reviewsJson = objectMapper.readTree(modelOutput)
                .path("content").get(0)
                .path("text").asText();

        return objectMapper.readValue(reviewsJson, new TypeReference<>() {});
    }

    private static void applyReviewMetadata(Review review, Map<String, SentimentWeight> productAssignedSentiments) {
        SentimentWeight sentimentWeight = productAssignedSentiments.get(review.getProduct().name());
        boolean isMajority = review.getSentiment() == sentimentWeight.getDominantSentiment();

        review.setReviewerReputation(isMajority
                ? ThreadLocalRandom.current().nextInt(MAJORITY_REPUTATION_MIN, MAJORITY_REPUTATION_MAX + 1)
                : ThreadLocalRandom.current().nextInt(MINORITY_REPUTATION_MIN, MINORITY_REPUTATION_MAX + 1));

        review.setVerifiedPurchase(isMajority
                ? ThreadLocalRandom.current().nextDouble() < MAJORITY_VERIFIED_CHANCE
                : ThreadLocalRandom.current().nextDouble() < MINORITY_VERIFIED_CHANCE);
    }

    // Simulates real user interaction by sending each review at random time intervals
    private static void publishToKafka(List<Review> reviews) throws InterruptedException {
        for (Review review : reviews) {
            review.setTimestamp(new Timestamp(System.currentTimeMillis()));

            try {
                String reviewJson = objectMapper.writeValueAsString(review);
                logger.info("{}\n", reviewJson);

                kafkaProducer.send(new ProducerRecord<>(KAFKA_TOPIC, reviewJson));
            } catch (Exception e) {
                logger.error(SEPARATOR);
                logger.error("Failed to publish review", e);
                logger.error(SEPARATOR);
            }
            Thread.sleep(ThreadLocalRandom.current().nextLong(SLEEP_START, SLEEP_END));
        }
    }
}