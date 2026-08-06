package com.jamiecalaku.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class FileLoader {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Map<String, Object> REVIEW_ANALYSIS_SCHEMA = loadJsonResource("review_analysis_schema.json");
    private static final String REVIEW_ANALYSIS_PROMPT = loadTextResource("review_analysis_prompt.txt");

    private static final Map<String, Object> DROP_DETECTION_SUMMARY_SCHEMA = loadJsonResource("product_drop_detection_summary_schema.json");
    private static final String DROP_DETECTION_SUMMARY_PROMPT = loadTextResource("product_drop_detection_summary_prompt.txt");

    private static String loadTextResource(String filename) {
        try (InputStream inputStream = FileLoader.class.getClassLoader().getResourceAsStream(filename)) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load resource: " + filename, e);
        }
    }

    private static Map<String, Object> loadJsonResource(String filename) {
        try (InputStream inputStream = FileLoader.class.getClassLoader().getResourceAsStream(filename)) {
            return objectMapper.readValue(inputStream, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load resource: " + filename, e);
        }
    }

    public static Map<String, Object> getReviewAnalysisSchema() {
        return REVIEW_ANALYSIS_SCHEMA;
    }

    public static String getReviewAnalysisPrompt() {
        return REVIEW_ANALYSIS_PROMPT;
    }

    public static Map<String, Object> getDropDetectionSummarySchema() {
        return DROP_DETECTION_SUMMARY_SCHEMA;
    }

    public static String getDropDetectionSummaryPrompt() {
        return DROP_DETECTION_SUMMARY_PROMPT;
    }

}
