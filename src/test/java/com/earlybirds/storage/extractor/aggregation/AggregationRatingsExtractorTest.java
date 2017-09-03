package com.earlybirds.storage.extractor.aggregation;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;
import scala.Serializable;

import static com.earlybirds.storage.schema.InputSchema.getSchema;
import static org.junit.Assert.assertEquals;

public class AggregationRatingsExtractorTest extends SharedJavaSparkContext implements Serializable {

    private static final String INPUT_DIR = "src/test/resources/input";

    @Test
    public void findMaxTimestampTest() throws Exception {
        SQLContext sqlContext = new SQLContext(sc());
        Dataset<Row> df = sqlContext.read()
                .schema(getSchema())
                .csv(INPUT_DIR + "/input1.csv");

        AggregationRatingsExtractor ratingsExtractor = new AggregationRatingsExtractor(null, null, 1);
        long maxTimestamp = ratingsExtractor.findMaxTimestamp(df);
        assertEquals(1476686550249L, maxTimestamp);
    }

    @Test
    public void computeRatingPenaltyTest() throws Exception {
        AggregationRatingsExtractor ratingsExtractor = new AggregationRatingsExtractor(null, null, 1);
        float penalty = ratingsExtractor.computeRatingPenalty(12f, 1476226549844L, 1476686549844L);
        assertEquals(9.28f, penalty, 0.01);
    }

    /**
     * TODO : Implement test for the other functions (I did not take time to create expected dataFrame to test each function)
     *
     */
}
