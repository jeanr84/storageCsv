package com.earlybirds.storage.extractor.lookup;


import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.File;
import java.io.IOException;

import static com.earlybirds.storage.extractor.lookup.LookupExtractorEnum.USER;
import static com.earlybirds.storage.schema.InputSchema.getSchema;

public class LookupExtractorTest extends SharedJavaSparkContext implements Serializable {

    private static final String OUTPUT_DIR = "src/test/resources/output";
    private static final String INPUT_DIR = "src/test/resources/input";
    private static final String EXPECTED_DIR = "src/test/resources/expected";

    @Test
    public void extractTest() throws Exception {
        SQLContext sqlContext = new SQLContext(sc());
        Dataset<Row> df = sqlContext.read()
                .schema(getSchema())
                .csv(INPUT_DIR + "/input1.csv");

        JavaPairRDD<Row, Long> resultExtract = LookupExtractorFactory.create(USER).extract(df);

        // Create the expected output
        JavaPairRDD<Row, Long> expectedRdd = jsc()
                .textFile(EXPECTED_DIR + "/expected1.csv")
                .mapToPair(line -> new Tuple2<>(RowFactory.create(line.split(",")[0]),
                        Long.parseLong(line.split(",")[1])));

        ClassTag<Tuple2<Row, Long>> tag =
                scala.reflect.ClassTag$.MODULE$
                        .apply(Tuple2.class);

        // Run the assertions on the result and expected
        JavaRDDComparisons.assertRDDEquals(
                JavaRDD.fromRDD(JavaPairRDD.toRDD(resultExtract), tag),
                JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRdd), tag));
    }

    @Test
    public void computeOnePartitionsTest() throws Exception {
        JavaPairRDD<Row, Long> inputPairRdd = jsc()
                .textFile(INPUT_DIR + "/input2.csv")
                .mapToPair(line -> new Tuple2<>(RowFactory.create(line.split(",")[0]),
                        Long.parseLong(line.split(",")[1])));

        LookupExtractor lookupExtractor = LookupExtractorFactory.create(USER);
        lookupExtractor.setMaxOutputPartitionSize(100);
        int numberOfPartitions = lookupExtractor.computeNumberOfPartitions(inputPairRdd);
        Assert.assertEquals(1, numberOfPartitions);
    }

    @Test
    public void computeTwoPartitionsTest() throws Exception {
        JavaPairRDD<Row, Long> inputPairRdd = jsc()
                .textFile(INPUT_DIR + "/input3.csv")
                .mapToPair(line -> new Tuple2<>(RowFactory.create(line.split(",")[0]),
                        Long.parseLong(line.split(",")[1])));

        LookupExtractor lookupExtractor = LookupExtractorFactory.create(USER);
        lookupExtractor.setMaxOutputPartitionSize(1);
        int numberOfPartitions = lookupExtractor.computeNumberOfPartitions(inputPairRdd);
        Assert.assertEquals(2, numberOfPartitions);
    }

    @Test
    public void saveTest() throws Exception {
        JavaPairRDD<Row, Long> inputPairRdd = jsc()
                .textFile(INPUT_DIR + "/input4.csv")
                .mapToPair(line -> new Tuple2<>(RowFactory.create(line.split(",")[0]),
                        Long.parseLong(line.split(",")[1])));

        LookupExtractor lookupExtractor = LookupExtractorFactory.create(USER);
        lookupExtractor.setMaxOutputPartitionSize(1);
        lookupExtractor.setOutputPath(OUTPUT_DIR + "/output4.csv");
        lookupExtractor.save(inputPairRdd);

        JavaRDD<String> result = jsc().textFile(OUTPUT_DIR + "/output4.csv/*");
        JavaRDD<String> expected = jsc().textFile(INPUT_DIR + "/input4.csv");

        JavaRDDComparisons.assertRDDEquals(result, expected);
    }

    @After
    public void deleteOutputFile() {
        try {
            File f = new File(OUTPUT_DIR);
            if (f.exists()) {
                FileUtils.cleanDirectory(f); //clean out directory (this is optional -- but good know)
                FileUtils.forceDelete(f); //delete directory
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}