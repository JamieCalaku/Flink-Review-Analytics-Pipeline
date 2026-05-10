package com.jamiecalaku.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jamiecalaku.llm.LLMClient;
import com.jamiecalaku.model.Review;
import com.jamiecalaku.utils.PromptLoader;
import com.jamiecalaku.utils.SentimentAssignment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

public class ReviewProducer {

    private enum Stage { STAGE_1, STAGE_2 }

    private static final KafkaProducer<String, String> kafkaProducer;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ReviewProducer.class);

    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<>(properties);
    }

    private static List<Review> generateReviewsWithPrompt(String prompt) throws JsonProcessingException {
        String llmOutput = LLMClient.invokeModel(prompt);

        Map<String, Object> responseBody = objectMapper.readValue(llmOutput, Map.class);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> responseContent = (List<Map<String, Object>>) responseBody.get("content");

        String reviewsJson = (String) responseContent.get(0).get("text");

        return objectMapper.readValue(reviewsJson, objectMapper.getTypeFactory().constructCollectionType(List.class, Review.class));
    }

    private static List<Review> generateReviews(Stage stage) throws JsonProcessingException {
        if (stage == Stage.STAGE_1) {
            String prompt = PromptLoader.loadReviewGenerationPrompt();
            prompt = prompt.replace("{IPHONESENTIMENT}", SentimentAssignment.getIphoneSentiment());
            prompt = prompt.replace("{MACBOOKSENTIMENT}", SentimentAssignment.getMacbookSentiment());
            prompt = prompt.replace("{AIRPODSSENTIMENT}", SentimentAssignment.getAirpodsSentiment());
            prompt = prompt.replace("{IPADSENTIMENT}", SentimentAssignment.getIpadSentiment());

            logger.info("Generating Stage 1 Reviews...");

            return generateReviewsWithPrompt(prompt);
        } else {
            String prompt = PromptLoader.loadReviewDropGenerationPrompt();
            prompt = prompt.replace("{IPHONESENTIMENT}",  SentimentAssignment.getDropSentiment(SentimentAssignment.getIphoneSentiment()));
            prompt = prompt.replace("{MACBOOKSENTIMENT}", SentimentAssignment.getDropSentiment(SentimentAssignment.getMacbookSentiment()));
            prompt = prompt.replace("{AIRPODSSENTIMENT}", SentimentAssignment.getDropSentiment(SentimentAssignment.getAirpodsSentiment()));
            prompt = prompt.replace("{IPADSENTIMENT}",    SentimentAssignment.getDropSentiment(SentimentAssignment.getIpadSentiment()));

            logger.info("Generating Stage 2 Reviews asynchronous...");

            return generateReviewsWithPrompt(prompt);
        }
    }

    public static void streamReviews() throws InterruptedException, JsonProcessingException {
        List<Review> stage1Reviews = generateReviews(Stage.STAGE_1);

        CompletableFuture<List<Review>> stage2Future = CompletableFuture.supplyAsync(() -> {
            try {
                return generateReviews(Stage.STAGE_2);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            logger.info("Stage 1 - Streaming reviews to Kafka Topic...\n\n\n\n");
            streamToKafka(stage1Reviews);

            logger.info("\n\n\n\nStage 2 — Simulating product drop and stream reviews to Kafka Topic...\n\n\n\n");
            streamToKafka(stage2Future.join());
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    private static void streamToKafka(List<Review> reviews) throws InterruptedException {
        for (Review review : reviews) {
            review.setTimestamp(System.currentTimeMillis());
            try {
                String reviewJson = objectMapper.writeValueAsString(review);
                logger.info("{}\n", reviewJson);
                kafkaProducer.send(new ProducerRecord<>("reviews", reviewJson));
            } catch (Exception e) {
                logger.error("Failed to stream review", e);
            }
            Thread.sleep(ThreadLocalRandom.current().nextLong(100, 2001));
        }
    }
}
