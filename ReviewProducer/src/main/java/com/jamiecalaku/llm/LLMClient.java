package com.jamiecalaku.llm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class LLMClient {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(LLMClient.class);


    private static final BedrockRuntimeClient bedrockClient = BedrockRuntimeClient.builder()
            .region(Region.EU_CENTRAL_1)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .httpClientBuilder(ApacheHttpClient.builder()
                    .socketTimeout(Duration.ofMinutes(4))
            )
            .build();

    private static final Map<String, Object> JSON_SCHEMA = Map.of(
            "type", "array",
            "items", Map.of(
                    "type", "object",
                    "additionalProperties", false,
                    "properties", Map.of(
                            "id", Map.of("type", "integer"),
                            "product", Map.of("type", "string", "enum", List.of("IPHONE", "MACBOOK", "AIRPODS", "IPAD")),
                            "sentiment", Map.of("type", "string", "enum", List.of("POSITIVE", "NEUTRAL", "NEGATIVE")),
                            "body", Map.of("type", "string"),
                            "helpfulVotes", Map.of("type", "integer"),
                            "verifiedPurchase", Map.of("type", "boolean")
                    ),
                    "required", List.of("id", "product", "sentiment", "body", "helpfulVotes", "verifiedPurchase")
            )
    );

    public static String invokeModel(String prompt) {
        try {
            Map<String, Object> requestBody = Map.of(
                    "messages", List.of(Map.of("role", "user", "content", prompt)),
                    "max_tokens", 10000,
                    "anthropic_version", "bedrock-2023-05-31",
                    "output_config", Map.of(
                            "format", Map.of(
                                    "type", "json_schema",
                                    "schema", JSON_SCHEMA
                            )
                    )
            );

            InvokeModelRequest request = InvokeModelRequest.builder()
                    .modelId("global.anthropic.claude-haiku-4-5-20251001-v1:0")
                    .contentType("application/json")
                    .accept("application/json")
                    .body(SdkBytes.fromUtf8String(objectMapper.writeValueAsString(requestBody)))
                    .build();

            logger.info("Invoking AWS Bedrock model {}...\n\n\n\n", request.modelId());

            InvokeModelResponse response = bedrockClient.invokeModel(request);
            return response.body().asUtf8String();

        } catch (Exception e) {
            throw new RuntimeException("LLM request failed", e);
        }
    }
}

