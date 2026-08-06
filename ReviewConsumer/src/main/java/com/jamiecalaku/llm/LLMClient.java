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
    private static final Logger logger = LoggerFactory.getLogger(LLMClient.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Region AWS_REGION = Region.of(System.getenv("CONSUMER_AWS_REGION"));
    private static final Duration AWS_TIMEOUT = Duration.ofMinutes(Integer.parseInt(System.getenv("CONSUMER_AWS_TIMEOUT")));
    private static final String AWS_MODEL_ID = System.getenv("CONSUMER_AWS_MODEL_ID");
    private static final int AWS_MODEL_MAX_TOKENS = Integer.parseInt(System.getenv("CONSUMER_AWS_MODEL_MAX_TOKENS"));

    private static final BedrockRuntimeClient bedrockClient = BedrockRuntimeClient.builder()
            .region(AWS_REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .httpClientBuilder(ApacheHttpClient.builder()
                    .socketTimeout(AWS_TIMEOUT)
            )
            .build();

    public static String invokeModel(String prompt, Map<String, Object> schema) {
        try {
            // Structured output via Bedrock with a json schema
            Map<String, Object> requestBody = Map.of(
                    "messages", List.of(Map.of("role", "user", "content", prompt)),
                    "max_tokens", AWS_MODEL_MAX_TOKENS,
                    "anthropic_version", "bedrock-2023-05-31",
                    "output_config", Map.of(
                            "format", Map.of(
                                    "type", "json_schema",
                                    "schema", schema
                            )
                    )
            );

            InvokeModelRequest request = InvokeModelRequest.builder()
                    .modelId(AWS_MODEL_ID)
                    .contentType("application/json")
                    .accept("application/json")
                    .body(SdkBytes.fromUtf8String(objectMapper.writeValueAsString(requestBody)))
                    .build();

            logger.info("Invoking AWS Bedrock model {}\n", request.modelId());

            InvokeModelResponse response = bedrockClient.invokeModel(request);
            return response.body().asUtf8String();
        } catch (Exception e) {
            throw new RuntimeException("LLM request failed", e);
        }
    }
}