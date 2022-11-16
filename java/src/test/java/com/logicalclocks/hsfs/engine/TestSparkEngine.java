package com.logicalclocks.hsfs.engine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;

public class TestSparkEngine {

    @Test
    public void testConvertToDefaultDataframe() {
        // Arrange
        SparkEngine sparkEngine = SparkEngine.getInstance();

        // Act
        StructType structType = new StructType();
        structType = structType.add("A", DataTypes.StringType, false);
        structType = structType.add("B", DataTypes.StringType, false);
        structType = structType.add("C", DataTypes.StringType, false);
        structType = structType.add("D", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("value1", "value2", "value3", "value4"));

        ArrayList<String> nonNullColumns = new ArrayList<>();
        nonNullColumns.add("a");

        Dataset<Row> dfOriginal = sparkEngine.getSparkSession().createDataFrame(nums, structType);

        Dataset<Row> dfConverted = sparkEngine.convertToDefaultDataframe(dfOriginal);

        StructType expected = new StructType();
        expected = expected.add("a", DataTypes.StringType, true);
        expected = expected.add("b", DataTypes.StringType, true);
        expected = expected.add("c", DataTypes.StringType, true);
        expected = expected.add("d", DataTypes.StringType, true);
        ArrayList<StructField> expectedJava = new ArrayList<>(JavaConverters.asJavaCollection(expected.toSeq()));

        ArrayList<StructField> dfConvertedJava = new ArrayList<>(JavaConverters.asJavaCollection(dfConverted.schema().toSeq()));
        // Assert
        for (int i = 0; i < expected.toList().length(); i++) {
            Assertions.assertEquals(expectedJava.get(i), dfConvertedJava.get(i));
        }
    }
}
