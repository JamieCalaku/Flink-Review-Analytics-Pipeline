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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
    private static final long SLEEP_START = Long.parseLong(System.getenv("PRODUCER_SLEEP_START"));
    private static final long SLEEP_END = Long.parseLong(System.getenv("PRODUCER_SLEEP_END"));

    private enum Stage {BASELINE, SENTIMENT_DROP}

    private static int reviewIdCounter = 1;

    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<>(properties);

        // Shutdown hook
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
            logger.info(SEPARATOR);
        }
    }

    private static List<Review> generateReviews(Stage stage) throws JsonProcessingException {
        String prompt = preparePrompt(stage);
        List<Review> reviews = invokeReviewGeneration(prompt);

        // Shuffle all the reviews for more realistic data and assign them an id
        Collections.shuffle(reviews, ThreadLocalRandom.current());
        for (Review review : reviews) {
            review.setId(reviewIdCounter++);
        }

        return reviews;
    }

    private static String preparePrompt(Stage stage){
        String prompt;
        int reviewCount;
        Map<String, SentimentWeight> productSentiments;

        if (stage == Stage.BASELINE) {
            prompt = FileLoader.getReviewGenerationPrompt();
            reviewCount = BASELINE_REVIEW_COUNT;

            productSentiments = Map.of(
                    "IPHONE",  SentimentAssignment.getIphoneSentiment(),
                    "MACBOOK", SentimentAssignment.getMacbookSentiment(),
                    "AIRPODS", SentimentAssignment.getAirpodsSentiment(),
                    "IPAD",    SentimentAssignment.getIpadSentiment()
            );
        } else {
            prompt = FileLoader.getReviewDropGenerationPrompt();
            reviewCount = DROP_REVIEW_COUNT;

            productSentiments = Map.of(
                    "IPHONE",  SentimentAssignment.getDropSentiment(SentimentAssignment.getIphoneSentiment()),
                    "MACBOOK", SentimentAssignment.getDropSentiment(SentimentAssignment.getMacbookSentiment()),
                    "AIRPODS", SentimentAssignment.getDropSentiment(SentimentAssignment.getAirpodsSentiment()),
                    "IPAD",    SentimentAssignment.getDropSentiment(SentimentAssignment.getIpadSentiment())
            );
        }

        int reviewsPerProduct = reviewCount / productSentiments.size();

        for (Map.Entry<String, SentimentWeight> entry : productSentiments.entrySet()) {
            String product = entry.getKey();
            Map<Sentiment, Integer> distribution = SentimentAssignment.distributeReviews(entry.getValue(), reviewsPerProduct);

            prompt = prompt.replace("{" + product + "_POSITIVE_COUNT}", String.valueOf(distribution.get(Sentiment.POSITIVE)));
            prompt = prompt.replace("{" + product + "_NEUTRAL_COUNT}",  String.valueOf(distribution.get(Sentiment.NEUTRAL)));
            prompt = prompt.replace("{" + product + "_NEGATIVE_COUNT}", String.valueOf(distribution.get(Sentiment.NEGATIVE)));
        }

        prompt = prompt.replace("{REVIEW_COUNT}", String.valueOf(reviewCount));
        return prompt;
    }


    private static List<Review> invokeReviewGeneration(String prompt) throws JsonProcessingException {
        String modelOutput = LLMClient.invokeModel(prompt);
        String reviewsJson = objectMapper.readTree(modelOutput)
                .path("content").get(0)
                .path("text").asText();

        return objectMapper.readValue(reviewsJson, new TypeReference<>() {});
    }

    // Simulates real user interaction by sending each review at random time intervals
    private static void publishToKafka(List<Review> reviews) throws InterruptedException {
        for (Review review : reviews) {
            review.setTimestamp(System.currentTimeMillis());
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