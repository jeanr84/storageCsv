package com.earlybirds.storage.extractor.aggregation;

import com.earlybirds.storage.extractor.Extractor;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

import static com.earlybirds.storage.schema.InputSchema.*;

@AllArgsConstructor
public class AggregationRatingsExtractor implements Extractor, Serializable {

    private Dataset<Row> userDf;
    private Dataset<Row> productDf;
    private int maxOutputPartitionSize; //In MB

    private static final String OUTPUT_PATH = "src/main/resources/extract/agg_ratings.csv";
    private static final String RATING_PENALTY_UDF = "RatingPenalty";
    private static final String RATING_PENALTY_COLUMN = "ratingAfterPenalty";
    private static final String RATING_SUM_COLUMN = "ratingSum";

    public void registerRatingPenaltyUDF(SQLContext sqlContext) {
        sqlContext.udf().register(
                RATING_PENALTY_UDF,
                (UDF3<Float, Long, Long, Float>) this::computeRatingPenalty,
                DataTypes.FloatType);
    }

    Float computeRatingPenalty(Float rating, Long timestamp, Long maxTimestamp) {
        return (float)(rating * Math.pow(0.95, Math.floor((maxTimestamp - timestamp) / 86_400_000.0))); //1000 * 3600 * 24 = 86400000
    }

    public void extractAndSave(Dataset<Row> df) {
        final long maxTimestamp = findMaxTimestamp(df);
        Dataset<Row> dfAfterPenalty = applyPenalty(df, maxTimestamp);
        Dataset<Row> dfSum = groupAndSum(dfAfterPenalty);
        Dataset<Row> dfFiltered = dfSum.where(dfSum.col(RATING_SUM_COLUMN).$greater(0.01));
        Dataset<Row> dfExtract = join(dfFiltered);
        dfExtract.cache();
        save(dfExtract);
        dfExtract.unpersist();
    }

    long findMaxTimestamp(Dataset<Row> df) {
        return df.agg(functions.max(TIMESTAMP_COLUMN)).head().getLong(0);
    }

    Dataset<Row> applyPenalty(Dataset<Row> df, long maxTimestamp) {
        return df.withColumn(
                RATING_PENALTY_COLUMN,
                functions.callUDF(RATING_PENALTY_UDF,
                        df.col(RATING_COLUMN),
                        df.col(TIMESTAMP_COLUMN),
                        functions.lit(maxTimestamp)
                ));
    }

    Dataset<Row> groupAndSum(Dataset<Row> df) {
        return df
                .groupBy(df.col(USER_ID_COLUMN), df.col(ITEM_ID_COLUMN))
                .sum(RATING_PENALTY_COLUMN).alias(RATING_PENALTY_COLUMN)
                .withColumnRenamed("sum(" + RATING_PENALTY_COLUMN + ")", RATING_SUM_COLUMN);
    }

    Dataset<Row> join(Dataset<Row> df) {
        return df.join(userDf, USER_ID_COLUMN)
                .join(productDf, ITEM_ID_COLUMN)
                .select(USER_ID_COLUMN + "AsInteger", ITEM_ID_COLUMN + "AsInteger", RATING_SUM_COLUMN);
    }

    private void save(Dataset<Row> df) {
        int numberOfPartitions = computeNumberOfPartitions(df);
        df.coalesce(numberOfPartitions).write().csv(OUTPUT_PATH);
    }

    int computeNumberOfPartitions(Dataset<Row> df) {
        long size = df.first().mkString().getBytes().length * df.count();
        return (int) Math.ceil((double) size / ((double) maxOutputPartitionSize * 1_000_000));
    }
}
