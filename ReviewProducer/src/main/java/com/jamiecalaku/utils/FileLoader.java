package com.jamiecalaku.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class FileLoader {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Map<String, Object> REVIEW_GENERATION_SCHEMA = loadJsonResource("review_generation_schema.json");
    private static final String REVIEW_GENERATION_PROMPT = loadTextResource("review_generation_prompt.txt");
    private static final String REVIEW_DROP_GENERATION_PROMPT = loadTextResource("review_drop_generation_prompt.txt");

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

    public static Map<String, Object> getReviewGenerationSchema() {
        return REVIEW_GENERATION_SCHEMA;
    }

    public static String getReviewGenerationPrompt() {
        return REVIEW_GENERATION_PROMPT;
    }

    public static String getReviewDropGenerationPrompt() {
        return REVIEW_DROP_GENERATION_PROMPT;
    }

}
