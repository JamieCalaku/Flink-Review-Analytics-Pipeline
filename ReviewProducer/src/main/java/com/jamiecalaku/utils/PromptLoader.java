package com.jamiecalaku.utils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PromptLoader {
    private static final String reviewGenerationPrompt = loadReviewGenerationPromptFromFile();
    private static final String reviewDropGenerationPrompt = loadReviewDropGenerationPromptFromFile();


    private static String loadReviewGenerationPromptFromFile() {
        try (InputStream inputStream = PromptLoader.class.getClassLoader()
                .getResourceAsStream("review_generation_prompt.txt")) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String loadReviewDropGenerationPromptFromFile() {
        try (InputStream inputStream = PromptLoader.class.getClassLoader()
                .getResourceAsStream("review_drop_generation_prompt.txt")) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String loadReviewGenerationPrompt() {
        return reviewGenerationPrompt;
    }

    public static String loadReviewDropGenerationPrompt() {
        return reviewDropGenerationPrompt;
    }

}
