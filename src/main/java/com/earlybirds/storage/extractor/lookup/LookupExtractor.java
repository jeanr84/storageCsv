package com.earlybirds.storage.extractor.lookup;

import com.earlybirds.storage.extractor.Extractor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class LookupExtractor implements Extractor {

    private final String columnName;
    private String outputPath;
    private int maxOutputPartitionSize; //In MB

    LookupExtractor(String columnName, String outputPath, int maxOutputPartitionSize) {
        this.columnName = columnName;
        this.outputPath = outputPath;
        this.maxOutputPartitionSize = maxOutputPartitionSize;
    }

    public void extractAndSave(Dataset<Row> df) {
        JavaPairRDD<Row, Long> extractPairRdd = extract(df);
        extractPairRdd.cache();
        save(extractPairRdd);
        extractPairRdd.unpersist();
    }

    JavaPairRDD<Row, Long> extract(Dataset<Row> df) {
        return df
                .select(df.col(columnName))
                .distinct()
                .toJavaRDD()
                .zipWithIndex();
    }

    void save(JavaPairRDD<Row, Long> extractPairRdd) {
        int numberOfPartitions = computeNumberOfPartitions(extractPairRdd);
        extractPairRdd
                .map(tuple -> tuple._1().mkString() + "," + tuple._2())
                .coalesce(numberOfPartitions)
                .saveAsTextFile(outputPath);
    }

    int computeNumberOfPartitions(JavaPairRDD<Row, Long> extractPairRdd) {
        long size = extractPairRdd.first()._1().getString(0).getBytes().length * extractPairRdd.count();
        return (int) Math.ceil((double) size / ((double) maxOutputPartitionSize * 1_000_000));
    }

    public Dataset<Row> getLookupDataframe(SQLContext sqlContext) {
        return sqlContext.read()
                .schema(getSchema())
                .csv(outputPath);
    }

    private StructType getSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(columnName, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(columnName + "AsInteger", DataTypes.IntegerType, false));
        return DataTypes.createStructType(fields);
    }
}
