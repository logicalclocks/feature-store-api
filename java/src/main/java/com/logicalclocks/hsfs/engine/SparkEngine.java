/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.util.Constants;
import lombok.Getter;
import org.apache.parquet.Strings;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
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

    // Configure the Spark context to allow dynamic partitions
    SparkContext sparkContext = sparkSession.sparkContext();
    SQLContext sqlContext = new SQLContext(sparkContext);

    sqlContext.setConf("hive.exec.dynamic.partition", "true");
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
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
