package com.jamiecalaku.pipeline;

import com.jamiecalaku.model.AnalysedReview;
import com.jamiecalaku.model.ProductRanking;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import com.jamiecalaku.utils.DatabaseConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProductRankingPipeline {
    private static final int CONSUMER_RANKING_DB_BATCH_SIZE = Integer.parseInt(System.getenv("CONSUMER_RANKING_DB_BATCH_SIZE"));
    private static final long CONSUMER_RANKING_DB_BATCH_INTERVAL = (long) (Double.parseDouble(System.getenv("CONSUMER_RANKING_DB_BATCH_INTERVAL")) * 1000);

    // Tracks totals per product
    public static class RankingState {
        public long totalReviews;
        public int sumScore;
        public double sumWeight;
        public double sumWeightedScore;
    }

    public static void build(DataStream<AnalysedReview> analysedReviewsStream) {
        DataStream<ProductRanking> productsRankingStream = analysedReviewsStream
                .keyBy(analysedReview -> analysedReview.getProduct().name())
                .process(new KeyedProcessFunction<>() {
                    private transient ValueState<RankingState> valueState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<RankingState> valueStateDescriptor = new ValueStateDescriptor<>("rankingState", RankingState.class);
                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(AnalysedReview analysedReview, Context context, Collector<ProductRanking> out) throws Exception {
                        RankingState rankingState = valueState.value();
                        if (rankingState == null) {
                            rankingState = new RankingState();
                        }

                        // sumScore ignores the weight and sumWeightedScore includes the weights
                        rankingState.totalReviews += 1;
                        rankingState.sumScore += analysedReview.getSentimentScore();
                        rankingState.sumWeight += analysedReview.getWeight();
                        rankingState.sumWeightedScore += (analysedReview.getSentimentScore() * analysedReview.getWeight());
                        valueState.update(rankingState);

                        // averageScore is just the average sentiment score that ignores weight and smartWeightedScore includes the weights
                        ProductRanking productRanking = new ProductRanking(
                                analysedReview.getProduct(),
                                rankingState.totalReviews,
                                rankingState.sumScore,
                                Math.round(rankingState.sumWeight * 100.0) / 100.0,
                                Math.round(((double) rankingState.sumScore / rankingState.totalReviews) * 100.0) / 100.0,
                                Math.round((rankingState.sumWeightedScore / rankingState.sumWeight) * 100.0) / 100.0,
                                analysedReview.getTimestamp()
                        );

                        out.collect(productRanking);
                    }
                });

        productsRankingStream.addSink(
                JdbcSink.sink(
                        """
                                INSERT INTO product_ranking
                                    (product, total_reviews, sum_score, sum_weight, average_score, smart_weighted_score, timestamp)
                                VALUES (?, ?, ?, ?, ?, ?, ?);
                            """,
                        (statement, ranking) -> {
                            statement.setString(1, ranking.getProduct().name());
                            statement.setLong(2, ranking.getTotalReviews());
                            statement.setInt(3, ranking.getSumScore());
                            statement.setDouble(4, ranking.getSumWeight());
                            statement.setDouble(5, ranking.getAverageScore());
                            statement.setDouble(6, ranking.getSmartWeightedScore());
                            statement.setTimestamp(7, ranking.getTimestamp());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(CONSUMER_RANKING_DB_BATCH_SIZE)
                                .withBatchIntervalMs(CONSUMER_RANKING_DB_BATCH_INTERVAL)
                                .build(),
                        DatabaseConfig.get()
                )
        );
    }
}