package com.logicalclocks.featurestore.engine;

import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkEngine {

  private static SparkEngine INSTANCE = null;

  public static synchronized SparkEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new SparkEngine();
    }
    return INSTANCE;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkEngine.class);

  @Getter
  private SparkSession sparkSession;

  private SparkEngine() {
    sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate();
  }

  public Dataset<Row> sql(String query) {
    return sparkSession.sql(query);
  }
}
