package com.jamiecalaku.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jamiecalaku.llm.LLMClient;
import com.jamiecalaku.model.AnalysedReview;
import com.jamiecalaku.model.AlertSeverity;
import com.jamiecalaku.model.DropAlert;
import com.jamiecalaku.model.Product;
import com.jamiecalaku.utils.DatabaseConfig;
import com.jamiecalaku.utils.FileLoader;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class ProductDropDetectionPipeline {
    private static final Logger logger = LoggerFactory.getLogger(ProductDropDetectionPipeline.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SEPARATOR = "================================================================================";

    private static final int CONSUMER_DROP_DETECTION_DB_BATCH_SIZE = Integer.parseInt(System.getenv("CONSUMER_DROP_DETECTION_DB_BATCH_SIZE"));
    private static final long CONSUMER_DROP_DETECTION_DB_BATCH_INTERVAL = (long) (Double.parseDouble(System.getenv("CONSUMER_DROP_DETECTION_DB_BATCH_INTERVAL")) * 1000);

    private static final int CONSUMER_DROP_DETECTION_BASELINE_SIZE = Integer.parseInt(System.getenv("CONSUMER_DROP_DETECTION_BASELINE_SIZE"));
    private static final int CONSUMER_DROP_DETECTION_WINDOW_SIZE = Integer.parseInt(System.getenv("CONSUMER_DROP_DETECTION_WINDOW_SIZE"));
    private static final double CONSUMER_DROP_DETECTION_THRESHOLD = Double.parseDouble(System.getenv("CONSUMER_DROP_DETECTION_THRESHOLD"));
    private static final long CONSUMER_DROP_DETECTION_TIMEOUT = (long) (Double.parseDouble(System.getenv("CONSUMER_DROP_DETECTION_TIMEOUT")) * 1000);

    private static final double CONSUMER_DROP_DETECTION_SEVERITY_CRITICAL = Double.parseDouble(System.getenv("CONSUMER_DROP_DETECTION_SEVERITY_CRITICAL"));
    private static final double CONSUMER_DROP_DETECTION_SEVERITY_HIGH = Double.parseDouble(System.getenv("CONSUMER_DROP_DETECTION_SEVERITY_HIGH"));
    private static final double CONSUMER_DROP_DETECTION_SEVERITY_MEDIUM = Double.parseDouble(System.getenv("CONSUMER_DROP_DETECTION_SEVERITY_MEDIUM"));

    public static class DropDetectionState {
        // Weighted running average on baseline reviews
        public double baselineWeightedSum;
        public double baselineWeightSum;
        public long baselineCount;

        // Sliding window of the last scores, their weights and bodies
        public List<Integer> recentScores = new ArrayList<>();
        public List<Double> recentWeights = new ArrayList<>();
        public List<String> recentBodies = new ArrayList<>();

        // LLM builds on the previous summary instead of starting from scratch
        public String lastSummary = "";

        // After an alert, skip the next reviews to prevent multiple alerts
        public int cooldown;

        // Timestamp of the last registered timer
        public long lastTimerTimestamp;
    }

    public static void build(DataStream<AnalysedReview> analysedReviewsStream) {
        DataStream<DropAlert> alertsStream = analysedReviewsStream
                .keyBy(analysedReview -> analysedReview.getProduct().name())
                .process(new KeyedProcessFunction<>() {
                    private transient ValueState<DropDetectionState> valueState;

                    @Override
                    public void open(Configuration parameters) {
                        valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("state", DropDetectionState.class)
                        );
                    }

                    @Override
                    public void processElement(AnalysedReview analysedReview, Context context, Collector<DropAlert> out) throws Exception {
                        DropDetectionState state = valueState.value();
                        if (state == null) {
                            state = new DropDetectionState();
                        }

                        // Cancel the previous timer and register a new one
                        if (state.lastTimerTimestamp > 0) {
                            context.timerService().deleteProcessingTimeTimer(state.lastTimerTimestamp);
                        }
                        state.lastTimerTimestamp = context.timerService().currentProcessingTime() + CONSUMER_DROP_DETECTION_TIMEOUT;
                        context.timerService().registerProcessingTimeTimer(state.lastTimerTimestamp);

                        // Build the baseline
                        if (state.baselineCount < CONSUMER_DROP_DETECTION_BASELINE_SIZE) {
                            state.baselineWeightedSum += analysedReview.getSentimentScore() * analysedReview.getWeight();
                            state.baselineWeightSum += analysedReview.getWeight();
                            state.baselineCount++;
                            valueState.update(state);
                            return;
                        }

                        // Sliding window
                        state.recentScores.add(analysedReview.getSentimentScore());
                        state.recentWeights.add(analysedReview.getWeight());
                        state.recentBodies.add(analysedReview.getBody());
                        if (state.recentScores.size() > CONSUMER_DROP_DETECTION_WINDOW_SIZE) {
                            state.recentScores.remove(0);
                            state.recentWeights.remove(0);
                            state.recentBodies.remove(0);
                        }

                        // Check if window is full before comparing
                        if (state.recentScores.size() < CONSUMER_DROP_DETECTION_WINDOW_SIZE) {
                            valueState.update(state);
                            return;
                        }

                        // Check if there is an active cooldown
                        if (state.cooldown > 0) {
                            state.cooldown--;
                            valueState.update(state);
                            return;
                        }

                        emitAlertDrop(state, analysedReview.getProduct(), out);
                        valueState.update(state);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext context, Collector<DropAlert> out) throws Exception {
                        DropDetectionState state = valueState.value();
                        if (state.recentScores.size() < CONSUMER_DROP_DETECTION_WINDOW_SIZE) {
                            return;
                        }

                        // Check if cooldown is not active, that means there are no new unprocessed reviews since the last alert
                        if (state.cooldown == 0) {
                            return;
                        }

                        Product product = Product.valueOf(context.getCurrentKey());

                        logger.debug("Timeout fired for {}, running final drop check", product.name());
                        state.cooldown = 0;

                        emitAlertDrop(state, product, out);
                        valueState.update(state);
                    }

                    private void emitAlertDrop(DropDetectionState state, Product product, Collector<DropAlert> out) {
                        double baselineAvg = state.baselineWeightedSum / state.baselineWeightSum;

                        double windowWeightedSum = 0;
                        double windowWeightSum = 0;

                        for (int i = 0; i < state.recentScores.size(); i++) {
                            windowWeightedSum += state.recentScores.get(i) * state.recentWeights.get(i);
                            windowWeightSum += state.recentWeights.get(i);
                        }

                        double currentAvg = windowWeightedSum / windowWeightSum;
                        double dropPercentage = Math.round(((baselineAvg - currentAvg) / baselineAvg) * 100.0 * 100.0) / 100.0;

                        // If there is no significant drop, update the baseline with the latest score
                        if (dropPercentage < CONSUMER_DROP_DETECTION_THRESHOLD) {
                            if (!state.recentScores.isEmpty()) {
                                int last = state.recentScores.size() - 1;
                                state.baselineWeightedSum += state.recentScores.get(last) * state.recentWeights.get(last);
                                state.baselineWeightSum += state.recentWeights.get(last);
                                state.baselineCount++;
                            }
                            return;
                        }

                        AlertSeverity alertSeverity = calculateSeverity(dropPercentage);
                        Timestamp alertTimestamp = new Timestamp(System.currentTimeMillis());

                        logger.info(SEPARATOR);
                        logger.info("Drop detected — product: {} baseline: {}, current: {}, drop: {}% ({})",
                                product.name(),
                                Math.round(baselineAvg * 100.0) / 100.0,
                                Math.round(currentAvg * 100.0) / 100.0,
                                dropPercentage,
                                alertSeverity);
                        logger.info(SEPARATOR + "\n");

                        String summary;
                        try {
                            summary = generateDropSummary(state.recentBodies, state.lastSummary);
                            state.lastSummary = summary;
                        } catch (Exception e) {
                            logger.error("Failed to generate summary with the LLM", e);
                            summary = "LLM summary failed";
                        }

                        DropAlert dropAlert = new DropAlert(
                                product,
                                Math.round(baselineAvg * 100.0) / 100.0,
                                Math.round(currentAvg * 100.0) / 100.0,
                                dropPercentage,
                                alertSeverity,
                                summary,
                                alertTimestamp
                        );

                        out.collect(dropAlert);

                        state.cooldown = CONSUMER_DROP_DETECTION_WINDOW_SIZE;
                    }
                });

        alertsStream.addSink(
                JdbcSink.sink(
                        """
                                INSERT INTO product_alerts
                                    (product, previous_average, current_average, drop_percentage, severity, summary, timestamp)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT (product)
                                DO UPDATE SET
                                    previous_average = EXCLUDED.previous_average,
                                    current_average = EXCLUDED.current_average,
                                    drop_percentage = EXCLUDED.drop_percentage,
                                    severity = EXCLUDED.severity,
                                    summary = EXCLUDED.summary,
                                    timestamp = EXCLUDED.timestamp;
                            """,
                        (statement, alert) -> {
                            statement.setString(1, alert.getProduct().name());
                            statement.setDouble(2, alert.getPreviousAverage());
                            statement.setDouble(3, alert.getCurrentAverage());
                            statement.setDouble(4, alert.getDropPercentage());
                            statement.setString(5, alert.getSeverity().name());
                            statement.setString(6, alert.getSummary());
                            statement.setTimestamp(7, alert.getTimestamp());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(CONSUMER_DROP_DETECTION_DB_BATCH_SIZE)
                                .withBatchIntervalMs(CONSUMER_DROP_DETECTION_DB_BATCH_INTERVAL)
                                .build(),
                        DatabaseConfig.get()
                )
        );
    }

    private static AlertSeverity calculateSeverity(double dropPercentage) {
        if (dropPercentage >= CONSUMER_DROP_DETECTION_SEVERITY_CRITICAL) return AlertSeverity.CRITICAL;
        if (dropPercentage >= CONSUMER_DROP_DETECTION_SEVERITY_HIGH) return AlertSeverity.HIGH;
        if (dropPercentage >= CONSUMER_DROP_DETECTION_SEVERITY_MEDIUM) return AlertSeverity.MEDIUM;
        return AlertSeverity.LOW;
    }

    private static String generateDropSummary(List<String> bodies, String lastSummary) throws JsonProcessingException {
        String prompt = preparePrompt(bodies, lastSummary);

        return invokeDropAnalysis(prompt);
    }

    private static String preparePrompt(List<String> bodies, String lastSummary) throws JsonProcessingException {
        String prompt = FileLoader.getDropDetectionSummaryPrompt();
        prompt = prompt.replace("{BODIES}", objectMapper.writeValueAsString(bodies));
        prompt = prompt.replace("{LAST_SUMMARY}", lastSummary.isEmpty() ? "None" : lastSummary);

        return prompt;
    }

    private static String invokeDropAnalysis(String prompt) throws JsonProcessingException {
        String modelOutput = LLMClient.invokeModel(prompt, FileLoader.getDropDetectionSummarySchema());
        String summaryJson = objectMapper.readTree(modelOutput)
                .path("content").get(0)
                .path("text").asText();

        return objectMapper.readTree(summaryJson).path("summary").asText();
    }
}