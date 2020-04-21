package com.logicalclocks.featurestore.engine;

import com.logicalclocks.featurestore.StorageConnector;
import com.logicalclocks.featurestore.util.Constants;
import io.netty.util.Constant;
import lombok.Getter;
import org.apache.parquet.Strings;
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

  public void configureConnector(StorageConnector storageConnector) {
    switch (storageConnector.getStorageConnectorType()) {
      case S3:
        configureS3Connector(storageConnector);
        break;
    }
  }

  public static String sparkPath(String path) {
    if (path.startsWith(Constants.S3_SCHEME)) {
      return path.replaceFirst(Constants.S3_SCHEME, Constants.S3_SPARK_SCHEME);
    }

    return path;
  }

  private void configureS3Connector(StorageConnector storageConnector) {
    if (!Strings.isNullOrEmpty(storageConnector.getAccessKey())) {
      sparkSession.conf().set("fs.s3a.access.key", storageConnector.getAccessKey());
      sparkSession.conf().set("fs.s3a.secret.key", storageConnector.getSecretKey());
    }
  }
}
