/*
 *  Copyright (c) 2023-2023. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.spark.util;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.util.Constants;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class StorageConnectorUtils {

  public Dataset<Row> read(StorageConnector connector, String query, String dataFormat,
                           Map<String, String> options, String path) throws FeatureStoreException, IOException {
    return SparkEngine.getInstance().read(connector,  dataFormat, options, path);
  }

  public Dataset<Row> read(StorageConnector.HopsFsConnector connector, String query,
                           String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
    return SparkEngine.getInstance().read(connector,  dataFormat, options, path);
  }

  public Dataset<Row> read(StorageConnector.S3Connector connector, String dataFormat,
                           Map<String, String> options, String path) throws FeatureStoreException, IOException {
    connector.update();
    return SparkEngine.getInstance().read(connector, dataFormat, options, path);
  }

  public Dataset<Row> read(StorageConnector.RedshiftConnector connector, String query,
                           String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
    connector.update();
    Map<String, String> readOptions = connector.sparkOptions();
    if (!Strings.isNullOrEmpty(query)) {
      readOptions.put("query", query);
    }
    return SparkEngine.getInstance().read(connector, Constants.JDBC_FORMAT, readOptions, null);
  }

  public Dataset<Row> read(StorageConnector.AdlsConnector connector, String query, String dataFormat,
                           Map<String, String> options, String path) throws FeatureStoreException, IOException {
    return SparkEngine.getInstance().read(connector, dataFormat, options, path);
  }

  public Dataset<Row> read(StorageConnector.SnowflakeConnector connector, String query, String dataFormat,
                           Map<String, String> options, String path) throws FeatureStoreException, IOException {
    Map<String, String> readOptions = connector.sparkOptions();
    if (!Strings.isNullOrEmpty(query)) {
      // if table also specified we override to use query
      readOptions.remove(Constants.SNOWFLAKE_TABLE);
      readOptions.put("query", query);
    }
    return SparkEngine.getInstance().read(connector, Constants.SNOWFLAKE_FORMAT, readOptions, null);
  }

  public Dataset<Row> read(StorageConnector.JdbcConnector connector, String query,
                           String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
    connector.update();
    Map<String, String> readOptions = connector.sparkOptions();
    if (!Strings.isNullOrEmpty(query)) {
      readOptions.put("query", query);
    }
    return SparkEngine.getInstance().read(connector.refetch(), Constants.JDBC_FORMAT, readOptions, null);
  }

  public Dataset<Row> read(StorageConnector.KafkaConnector connector, String query, String dataFormat,
                           Map<String, String> options, String path) {
    throw new NotSupportedException("Reading a Kafka Stream into a static Spark Dataframe is not supported.");
  }

  public Dataset<Row> read(StorageConnector.GcsConnector connector, String query, String dataFormat,
                           Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
    SparkEngine.getInstance().setupConnectorHadoopConf(connector);
    return SparkEngine.getInstance().read(connector, dataFormat, options, path);
  }

  public Dataset<Row> read(StorageConnector.BigqueryConnector connector, String query, String dataFormat,
                           Map<String, String> options, String path)
      throws FeatureStoreException, IOException {

    Map<String, String> readOptions = connector.sparkOptions();
    // merge user spark options on top of default spark options
    if (options != null && !options.isEmpty()) {
      readOptions.putAll(options);
    }

    if (!Strings.isNullOrEmpty(query)) {
      path = query;
    } else if (!Strings.isNullOrEmpty(connector.getQueryTable())) {
      path = connector.getQueryTable();
    } else if (!Strings.isNullOrEmpty(path)) {
      path = path;
    } else {
      throw new IllegalArgumentException("Either query should be provided"
          + " or Query Project,Dataset and Table should be set");
    }

    return SparkEngine.getInstance().read(connector, Constants.BIGQUERY_FORMAT, readOptions, path);
  }

  public Dataset<Row> readStream(StorageConnector.KafkaConnector connector, String topic, boolean topicPattern,
                                 String messageFormat, String schema, Map<String, String> options,
                                 boolean includeMetadata) throws FeatureStoreException, IOException {
    if (!Arrays.asList("avro", "json", null).contains(messageFormat.toLowerCase())) {
      throw new IllegalArgumentException("Can only read JSON and AVRO encoded records from Kafka.");
    }

    connector.setSslTruststoreLocation(SparkEngine.getInstance().addFile(connector.getSslTruststoreLocation()));
    connector.setSslKeystoreLocation(SparkEngine.getInstance().addFile(connector.getSslTruststoreLocation()));

    if (topicPattern) {
      options.put("subscribePattern", topic);
    } else {
      options.put("subscribe", topic);
    }

    return SparkEngine.getInstance().readStream(connector, connector.sparkFormat,
        messageFormat.toLowerCase(), schema, options, includeMetadata);
  }
}
