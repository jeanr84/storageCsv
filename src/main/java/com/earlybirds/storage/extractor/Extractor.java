package com.earlybirds.storage.extractor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Extractor {

    void extractAndSave(Dataset<Row> df);
}
