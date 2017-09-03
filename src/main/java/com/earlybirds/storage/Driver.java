package com.earlybirds.storage;

import com.earlybirds.storage.extractor.aggregation.AggregationRatingsExtractor;
import com.earlybirds.storage.extractor.lookup.LookupExtractor;
import com.earlybirds.storage.extractor.lookup.LookupExtractorFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import static com.earlybirds.storage.extractor.lookup.LookupExtractorEnum.PRODUCT;
import static com.earlybirds.storage.extractor.lookup.LookupExtractorEnum.USER;
import static com.earlybirds.storage.schema.InputSchema.getSchema;

@Slf4j
public class Driver {

    public static void main(String ... args) {

        long start = System.currentTimeMillis();

        if (args.length != 1) {
            log.warn("Correct usage : One argument - the path of the input file");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setMaster("local").setAppName("storageCsv");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> df = sqlContext.read()
                .schema(getSchema())
                .csv(args[0])
                .cache();

        LookupExtractor userLookupExtractor = LookupExtractorFactory.create(USER);
        LookupExtractor productLookupExtractor = LookupExtractorFactory.create(PRODUCT);
        userLookupExtractor.extractAndSave(df);
        productLookupExtractor.extractAndSave(df);

        Dataset<Row> userDf = userLookupExtractor.getLookupDataframe(sqlContext);
        Dataset<Row> productDf = productLookupExtractor.getLookupDataframe(sqlContext);

        AggregationRatingsExtractor ratingsExtractor = new AggregationRatingsExtractor(userDf, productDf, 200);
        ratingsExtractor.registerRatingPenaltyUDF(sqlContext);
        ratingsExtractor.extractAndSave(df);

        long duration = System.currentTimeMillis() - start;
        String durationString = DurationFormatUtils.formatDuration(duration, "HH:mm:ss,SSS");
        log.info("Duration : {}", durationString);
    }
}
