package com.earlybirds.storage.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class InputSchema {

    public static final String USER_ID_COLUMN = "userId";
    public static final String ITEM_ID_COLUMN = "itemId";
    public static final String RATING_COLUMN = "rating";
    public static final String TIMESTAMP_COLUMN = "timestamp";

    public static StructType getSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(USER_ID_COLUMN, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(ITEM_ID_COLUMN, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(RATING_COLUMN, DataTypes.FloatType, false));
        fields.add(DataTypes.createStructField(TIMESTAMP_COLUMN, DataTypes.LongType, false));
        return DataTypes.createStructType(fields);
    }
}
