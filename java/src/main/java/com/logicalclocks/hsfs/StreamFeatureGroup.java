/*
 * Copyright (c) 2021. Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.CodeEngine;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.engine.StreamFeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  private Boolean onlineEnabled;

  @Getter
  @Setter
  private StorageConnector onlineStorageConnector;

  @Getter
  @Setter
  private StorageConnector offlineStorageConnector;

  @Getter
  @Setter
  private String type = "streamFeatureGroupDTO";

  @Getter
  @Setter
  protected String location;

  @Getter
  @Setter
  private List<String> statisticColumns;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  @JsonIgnore
  // This is only used in the client. In the server they are aggregated in the `features` field
  private String hudiPrecombineKey;

  @Getter(onMethod = @__(@Override))
  @Setter
  private String onlineTopicName;

  @Setter
  private DeltaStreamerJobConf deltaStreamerJobConf;

  private final StreamFeatureGroupEngine streamFeatureGroupEngine = new StreamFeatureGroupEngine();
  private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamFeatureGroup.class);

  @Builder
  public StreamFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                            List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                            boolean onlineEnabled, List<Feature> features,
                            StatisticsConfig statisticsConfig, String onlineTopicName, String eventTime) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
                ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
                ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = hudiPrecombineKey != null ? hudiPrecombineKey.toLowerCase() : null;
    this.onlineEnabled = onlineEnabled;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.onlineTopicName = onlineTopicName;
    this.eventTime = eventTime;
  }

  // used for updates
  public StreamFeatureGroup(Integer id, String description, List<Feature> features) {
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public StreamFeatureGroup(FeatureStore featureStore, int id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  /**
   * Read the feature group into a dataframe.
   * Reads the feature group by from the offline storage as Spark DataFrame on Hopsworks and Databricks.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get stream feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // read feature group
   *        fg.read()
   * }
   * </pre>
   *
   * @return DataFrame.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no commit information was found
   *                               for this feature group;
   * @throws IOException Generic IO exception.
   */
  public Object read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  /**
   * Read the stream feature group into a dataframe.
   * Reads the stream feature group from the offline or online storage as Spark DataFrame on Hopsworks and Databricks.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // read feature group data from online storage
   *        fg.read(true)
   *        // read feature group data from offline storage
   *        fg.read(false)
   * }
   * </pre>
   *
   * @param online Set `online` to `true` to read from the online storage.
   * @return Spark DataFrame containing the feature data.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no commit information was found
   *                               for this feature group;
   * @throws IOException Generic IO exception.
   */
  public Object read(boolean online) throws FeatureStoreException, IOException {
    return read(online, null);
  }

  /**
   * Read the stream feature group into a dataframe.
   * Reads the stream feature group by from the offline or online storage as Spark DataFrame on Hopsworks and
   * Databricks.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get stream feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional read options
   *        Map<String, String> readOptions = ...;
   *        // read feature group data
   *        fg.read(readOptions)
   * }
   * </pre>
   *
   * @param online Set `online` to `true` to read from the online storage.
   * @param readOptions Additional read options as key/value pairs.
   * @return Spark DataFrame containing the feature data.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no commit information was found
   *                               for this feature group;
   * @throws IOException Generic IO exception.
   */
  public Object read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return selectAll().read(online, readOptions);
  }

  /**
   * Reads stream Feature group into a dataframe at a specific point in time.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // read feature group data as of specific point in time (Hudi commit timestamp).
   *        fg.read("20230205210923")
   * }
   * </pre>
   *
   * @param wallclockTime Read data as of this point in time. Datetime string. The String should be formatted in one of
   *                      the following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return Spark DataFrame containing feature data.
   * @throws FeatureStoreException In case it's unable to identify format of the provided wallclockTime date format
   * @throws IOException  Generic IO exception.
   * @throws ParseException In case it's unable to parse provided wallclockTime to date type.
   */
  public Object read(String wallclockTime)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime).read(false, null);
  }

  /**
   * Reads stream Feature group into a dataframe at a specific point in time.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional read options
   *        Map<String, String> readOptions = ...;
   *        // read stream feature group data as of specific point in time (Hudi commit timestamp).
   *        fg.read("20230205210923", readOptions)
   * }
   * </pre>
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param readOptions Additional read options as key-value pairs.
   * @return Spark DataFrame containing feature data.
   * @throws FeatureStoreException In case it's unable to identify format of the provided wallclockTime date format
   * @throws IOException  Generic IO exception.
   * @throws ParseException In case it's unable to parse provided wallclockTime to date type.
   */
  public Object read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime).read(false, readOptions);
  }

  @Deprecated
  public Object readChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, null);
  }

  @Deprecated
  public Object readChanges(String wallclockStartTime, String wallclockEndTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, readOptions);
  }

  /**
   * Get Query object to retrieve all features of the group at a point in the past.
   * This method selects all features in the feature group and returns a Query object
   * at the specified point in time. This can then either be read into a Dataframe
   * or used further to perform joins or construct a training dataset.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // get query object to retrieve stream feature group feature data as of
   *        // specific point in time (Hudi commit timestamp).
   *        fg.asOf("20230205210923")
   * }
   * </pre>
   *
   * @param wallclockTime Read data as of this point in time. Datetime string. The String should be formatted in one of
   *                      the following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException In case it's unable to identify format of the provided wallclockTime date format
   * @throws ParseException In case it's unable to parse provided wallclockTime to date type.
   */
  public Query asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return selectAll().asOf(wallclockTime);
  }

  /**
   * Get Query object to retrieve all features of the group at a point in the past.
   * This method selects all features in the feature group and returns a Query object
   * at the specified point in time. This can then either be read into a Dataframe
   * or used further to perform joins or construct a training dataset.
   *
   * <pre>
   * {@code // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // get query object to retrieve feature group feature data as of specific point in time "20230205210923"
   *        // but exclude commits until "20230204073411" (Hudi commit timestamp).
   *        fg.asOf("20230205210923", "20230204073411")
   * }
   * </pre>
   *
   * @param wallclockTime Read data as of this point in time. Datetime string. The String should be formatted in one of
   *                      the following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param excludeUntil Exclude commits until this point in time. Datetime string. The String should be formatted in
   *                     one of the following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException In case it's unable to identify format of the provided wallclockTime date format
   * @throws ParseException In case it's unable to parse provided wallclockTime to date type.
   */
  public Query asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    return selectAll().asOf(wallclockTime, excludeUntil);
  }

  @Deprecated
  public <S> void save(S featureData, Map<String, String> writeOptions)
          throws FeatureStoreException, IOException, ParseException {
    streamFeatureGroupEngine.save(this, featureData, partitionKeys, hudiPrecombineKey, writeOptions, null);
    codeEngine.saveCode(this);
  }

  @Deprecated
  public <S> void save(S featureData, Map<String, String> writeOptions, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {
    streamFeatureGroupEngine.save(this, featureData, partitionKeys, hudiPrecombineKey, writeOptions,
        jobConfiguration);
    codeEngine.saveCode(this);
  }

  /**
   * Persist the metadata and materialize the stream feature group to the feature store or insert data from a
   * dataframe into the existing stream feature group.
   * Incrementally insert data to a stream feature group or overwrite all  data contained in the feature group.
   * The `features` dataframe can be a Spark DataFrame or RDD.
   * If the stream feature group doesn't exist, the insert method will create the necessary metadata the first time it
   * is invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        //insert feature data
   *        fg.insert(featureData);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData spark DataFrame, RDD. Features to be saved.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> void insert(S featureData) throws FeatureStoreException, IOException, ParseException {
    insert(featureData, SaveMode.APPEND, null, null);
  }

  /**
   * Persist the metadata and materialize the stream feature group to the feature store or insert data from a
   * dataframe into the existing stream feature group.
   * Incrementally insert data to a stream feature group or overwrite all  data contained in the feature group.
   * The `features` dataframe can be a Spark DataFrame or RDD.
   * If the stream feature group doesn't exist, the insert method will create the necessary metadata the first time it
   * is invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional write options
   *        Map<String, String> writeOptions = ...;
   *        // insert feature data
   *        fg.insert(featureData, writeOptions);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param writeOptions Additional write options as key-value pairs.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> void insert(S featureData, Map<String, String> writeOptions) throws FeatureStoreException, IOException,
      ParseException {
    insert(featureData, SaveMode.APPEND, writeOptions, null);
  }

  /**
   * Persist the metadata and materialize the stream feature group to the feature store or insert data from a
   * dataframe into the existing stream feature group.
   * Incrementally insert data to a stream feature group or overwrite all  data contained in the feature group.
   * The `features` dataframe can be a Spark DataFrame or RDD.
   * If the stream feature group doesn't exist, the insert method will create the necessary metadata the first time it
   * is invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // Define job configuration.
   *        JobConfiguration jobConfiguration = ...;
   *        // insert feature data
   *        fg.insert(featureData, jobConfiguration);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param jobConfiguration configure the Hopsworks Job used to write data into the stream feature group.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> void insert(S featureData, JobConfiguration jobConfiguration) throws FeatureStoreException, IOException,
      ParseException {
    insert(featureData, SaveMode.APPEND, null, jobConfiguration);
  }

  /**
   * Persist the metadata and materialize the stream feature group to the feature store or insert data from a
   * dataframe into the existing stream feature group.
   * Incrementally insert data to a stream feature group or overwrite all  data contained in the feature group.
   * The `features` dataframe can be a Spark DataFrame or RDD.
   * If the stream feature group doesn't exist, the insert method will create the necessary metadata the first time it
   * is invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional write options
   *        Map<String, String> writeOptions = ...;
   *        // define job configuration.
   *        JobConfiguration jobConfiguration = ...;
   *        // insert feature data
   *        fg.insert(featureData, writeOptions);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param saveMode org.apache.spark.sql.saveMode: APPEND, UPSERT, OVERWRITE
   * @param writeOptions Additional write options as key-value pairs.
   * @param jobConfiguration configure the Hopsworks Job used to write data into the stream feature group.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> void insert(S featureData, SaveMode saveMode,
                         Map<String, String> writeOptions, JobConfiguration jobConfiguration)
          throws FeatureStoreException, IOException, ParseException {
    streamFeatureGroupEngine.insert(this, featureData, saveMode,  partitionKeys,
        hudiPrecombineKey, writeOptions, jobConfiguration);
    codeEngine.saveCode(this);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // insert feature data
   *        fg.insertStream(featureData);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @return Streaming Query object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> Object insertStream(S featureData) throws FeatureStoreException, IOException, ParseException {
    return insertStream(featureData, null, "append", false, null, null, null);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // insert feature data
   *        fg.insertStream(featureData, queryName);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param queryName Specify a name for the query to make it easier to recognise in the Spark UI
   * @return Streaming Query object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> Object insertStream(S featureData, String queryName)
      throws FeatureStoreException, IOException, ParseException {
    return insertStream(featureData, queryName, null,false, null, null, null);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional write options
   *        Map<String, String> writeOptions = ...;
   *        // insert feature data
   *        fg.insertStream(featureData, writeOptions);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param writeOptions Additional write options as key-value pairs.
   * @return Streaming Query object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> Object insertStream(S featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    return insertStream(featureData, null, null, false, null, null, writeOptions);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long-running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional write options
   *        Map<String, String> writeOptions = ...;
   *        // insert feature data
   *        fg.insertStream(featureData, queryName);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param queryName Specify a name for the query to make it easier to recognise in the Spark UI
   * @param writeOptions Additional write options as key-value pairs.
   * @return Streaming Query object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> Object insertStream(S featureData, String queryName, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    return insertStream(featureData, queryName, "append", false, null, null, writeOptions);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long-running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // insert feature data
   *        String queryName = ...;
   *        String outputMode = ...;
   *        fg.insertStream(featureData, queryName outputMode);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param queryName Specify a name for the query to make it easier to recognise in the Spark UI
   * @param outputMode Specifies how data of a streaming DataFrame/Dataset is
   *                 written to a streaming sink. (1) `"append"`: Only the new rows in the
   *                 streaming DataFrame/Dataset will be written to the sink. (2)
   *                 `"complete"`: All the rows in the streaming DataFrame/Dataset will be
   *                 written to the sink every time there is some update. (3) `"update"`:
   *                 only the rows that were updated in the streaming DataFrame/Dataset will
   *                 be written to the sink every time there are some updates.
   *                 If the query doesn’t contain aggregations, it will be equivalent to
   *                 append mode. Default  behaviour is `"append"`.
   * @return Streaming Query object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> Object insertStream(S featureData, String queryName, String outputMode)
      throws FeatureStoreException, IOException, ParseException {
    return insertStream(featureData, queryName, outputMode, false, null, null, null);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long-running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // insert feature data
   *        String queryName = ...;
   *        String outputMode = ...;
   *        String checkpointLocation = ...;
   *        fg.insertStream(featureData, queryName outputMode, checkpointLocation);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param queryName Specify a name for the query to make it easier to recognise in the Spark UI
   * @param outputMode Specifies how data of a streaming DataFrame/Dataset is
   *                 written to a streaming sink. (1) `"append"`: Only the new rows in the
   *                 streaming DataFrame/Dataset will be written to the sink. (2)
   *                 `"complete"`: All the rows in the streaming DataFrame/Dataset will be
   *                 written to the sink every time there is some update. (3) `"update"`:
   *                 only the rows that were updated in the streaming DataFrame/Dataset will
   *                 be written to the sink every time there are some updates.
   *                 If the query doesn’t contain aggregations, it will be equivalent to
   *                 append mode.
   * @param checkpointLocation Checkpoint directory location. This will be used to as a reference to
   *                 from where to resume the streaming job.
   * @return Streaming Query object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> Object insertStream(S featureData, String queryName, String outputMode, String checkpointLocation)
      throws FeatureStoreException, IOException, ParseException {
    return insertStream(featureData, queryName, outputMode, false, null, checkpointLocation, null);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long-running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // insert feature data
   *        String queryName = ...;
   *        String outputMode = ...;
   *        fg.insertStream(featureData, queryName, outputMode, outputMode, true, 1000);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param queryName Specify a name for the query to make it easier to recognise in the Spark UI
   * @param outputMode Specifies how data of a streaming DataFrame/Dataset is
   *                 written to a streaming sink. (1) `"append"`: Only the new rows in the
   *                 streaming DataFrame/Dataset will be written to the sink. (2)
   *                 `"complete"`: All the rows in the streaming DataFrame/Dataset will be
   *                 written to the sink every time there is some update. (3) `"update"`:
   *                 only the rows that were updated in the streaming DataFrame/Dataset will
   *                 be written to the sink every time there are some updates.
   *                 If the query doesn’t contain aggregations, it will be equivalent to
   *                 append mode.
   * @param awaitTermination  Waits for the termination of this query, either by
   *                 query.stop() or by an exception. If the query has terminated with an
   *                 exception, then the exception will be thrown. If timeout is set, it
   *                 returns whether the query has terminated or not within the timeout
   *                 seconds
   * @param timeout Only relevant in combination with `awaitTermination=true`.
   * @return Streaming Query object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
                                 Long timeout) throws FeatureStoreException, IOException, ParseException {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, null, null);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long-running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // insert feature data
   *        String queryName = ...;
   *        String outputMode = ...;
   *        String checkpointLocation = ...;
   *        fg.insertStream(featureData, queryName, outputMode, outputMode, true, 1000, checkpointLocation);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param queryName Specify a name for the query to make it easier to recognise in the Spark UI
   * @param outputMode Specifies how data of a streaming DataFrame/Dataset is
   *                 written to a streaming sink. (1) `"append"`: Only the new rows in the
   *                 streaming DataFrame/Dataset will be written to the sink. (2)
   *                 `"complete"`: All the rows in the streaming DataFrame/Dataset will be
   *                 written to the sink every time there is some update. (3) `"update"`:
   *                 only the rows that were updated in the streaming DataFrame/Dataset will
   *                 be written to the sink every time there are some updates.
   *                 If the query doesn’t contain aggregations, it will be equivalent to
   *                 append mode.
   * @param awaitTermination  Waits for the termination of this query, either by
   *                 query.stop() or by an exception. If the query has terminated with an
   *                 exception, then the exception will be thrown. If timeout is set, it
   *                 returns whether the query has terminated or not within the timeout
   *                 seconds
   * @param timeout Only relevant in combination with `awaitTermination=true`.
   * @param checkpointLocation Checkpoint directory location. This will be used to as a reference to
   *                 from where to resume the streaming job.
   * @return Streaming Query object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
                                 Long timeout, String checkpointLocation)
      throws FeatureStoreException, IOException, ParseException {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, checkpointLocation, null);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long-running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional write options
   *        Map<String, String> writeOptions = ...;
   *        // insert feature data
   *        String queryName = ...;
   *        String outputMode = ...;
   *        String checkpointLocation = ...;
   *        fg.insertStream(featureData, queryName, outputMode, outputMode, true, 1000, checkpointLocation,
   *        writeOptions);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param queryName Specify a name for the query to make it easier to recognise in the Spark UI
   * @param outputMode Specifies how data of a streaming DataFrame/Dataset is
   *                 written to a streaming sink. (1) `"append"`: Only the new rows in the
   *                 streaming DataFrame/Dataset will be written to the sink. (2)
   *                 `"complete"`: All the rows in the streaming DataFrame/Dataset will be
   *                 written to the sink every time there is some update. (3) `"update"`:
   *                 only the rows that were updated in the streaming DataFrame/Dataset will
   *                 be written to the sink every time there are some updates.
   *                 If the query doesn’t contain aggregations, it will be equivalent to
   *                 append mode.
   * @param awaitTermination  Waits for the termination of this query, either by
   *                 query.stop() or by an exception. If the query has terminated with an
   *                 exception, then the exception will be thrown. If timeout is set, it
   *                 returns whether the query has terminated or not within the timeout
   *                 seconds
   * @param timeout Only relevant in combination with `awaitTermination=true`.
   * @param checkpointLocation Checkpoint directory location. This will be used to as a reference to
   *                 from where to resume the streaming job.
   * @param writeOptions Additional write options as key-value pairs.
   * @return Streaming Query object.
   */
  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
                                 Long timeout,  String checkpointLocation, Map<String, String> writeOptions) {

    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, checkpointLocation, writeOptions,
        null);
  }

  /**
   * Ingest a Spark Structured Streaming Dataframe to the online feature store.
   * This method creates a long-running Spark Streaming Query, you can control the termination of the query through the
   * arguments
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional write options
   *        Map<String, String> writeOptions = ...;
   *        JobConfiguration jobConfiguration = ...;
   *        String queryName = ...;
   *        String outputMode = ...;
   *        String checkpointLocation = ...;
   *        // insert feature data
   *        fg.insertStream(featureData, queryName, outputMode, outputMode, true, 1000, checkpointLocation,
   *        writeOptions, jobConfiguration);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Features in Streaming Dataframe to be saved.
   * @param queryName Specify a name for the query to make it easier to recognise in the Spark UI
   * @param outputMode Specifies how data of a streaming DataFrame/Dataset is
   *                 written to a streaming sink. (1) `"append"`: Only the new rows in the
   *                 streaming DataFrame/Dataset will be written to the sink. (2)
   *                 `"complete"`: All the rows in the streaming DataFrame/Dataset will be
   *                 written to the sink every time there is some update. (3) `"update"`:
   *                 only the rows that were updated in the streaming DataFrame/Dataset will
   *                 be written to the sink every time there are some updates.
   *                 If the query doesn’t contain aggregations, it will be equivalent to
   *                 append mode.
   * @param awaitTermination  Waits for the termination of this query, either by
   *                 query.stop() or by an exception. If the query has terminated with an
   *                 exception, then the exception will be thrown. If timeout is set, it
   *                 returns whether the query has terminated or not within the timeout
   *                 seconds
   * @param timeout Only relevant in combination with `awaitTermination=true`.
   * @param checkpointLocation Checkpoint directory location. This will be used to as a reference to
   *                 from where to resume the streaming job.
   * @param writeOptions Additional write options as key-value pairs.
   * @param jobConfiguration configure the Hopsworks Job used to write data into the stream feature group.
   * @return Streaming Query object.
   */
  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
                                 Long timeout,  String checkpointLocation, Map<String, String> writeOptions,
                                 JobConfiguration jobConfiguration) {

    return streamFeatureGroupEngine.insertStream(this, featureData, queryName, outputMode,
        awaitTermination, timeout, checkpointLocation,  partitionKeys, hudiPrecombineKey, writeOptions,
        jobConfiguration);
  }

  /**
   * Drops records present in the provided DataFrame and commits it as update to this Stream Feature group.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // drop records of feature data and commit
   *        fg.commitDeleteRecord(featureData);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Spark DataFrame, RDD. Feature data to be deleted.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> void commitDeleteRecord(S featureData)
      throws FeatureStoreException, IOException, ParseException {
    utils.commitDelete(this, featureData, null);
  }

  /**
   * Drops records present in the provided DataFrame and commits it as update to this Stream Feature group.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // define additional write options
   *        Map<String, String> writeOptions = ...;
   *        // drop records of feature data and commit
   *        fg.commitDeleteRecord(featureData, writeOptions);
   * }
   * </pre>
   *
   * @param <S> generic type for dataframes, can be Spark or Flink streaming dataframes.
   * @param featureData Spark DataFrame, RDD. Feature data to be deleted.
   * @param writeOptions Additional write options as key-value pairs.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public <S> void commitDeleteRecord(S featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    utils.commitDelete(this, featureData, writeOptions);
  }

  /**
   * Retrieves commit timeline for this stream feature group.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // get commit timeline
   *        fg.commitDetails();
   * }
   * </pre>
   *
   * @return commit details.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetails(this, null);
  }

  /**
   /**
   * Retrieves commit timeline for this stream feature group.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        // get latest 10 commit details
   *        fg.commitDetails(10);
   * }
   * </pre>
   *
   * @param limit number of commits to return.
   * @return commit details.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetails(this, limit);
  }

  /**
   * Return commit details as of specific point in time.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature group handle
   *        StreamFeatureGroup fg = ...;
   *        //get commit details as of 20230206
   *        fg.commitDetails("20230206");
   * }
   * </pre>
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetailsByWallclockTime(this, wallclockTime, null);
  }

  /**
   * Return commit details as of specific point in time.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature group handle
   *        FeatureGroup fg = ...;
   *        // get top 10 commit details as of 20230206
   *        fg.commitDetails("20230206", 10);
   * }
   * </pre>
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param limit number of commits to return.
   * @return commit details.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetailsByWallclockTime(this, wallclockTime, limit);
  }

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    return getSubject().getSchema();
  }

  @JsonIgnore
  public List<String> getComplexFeatures() {
    return utils.getComplexFeatures(features);
  }

  @JsonIgnore
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    return utils.getFeatureAvroSchema(featureName, utils.getDeserializedAvroSchema(getAvroSchema()));
  }

  @JsonIgnore
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getEncodedAvroSchema(getDeserializedAvroSchema(), utils.getComplexFeatures(features));
  }

  @JsonIgnore
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getDeserializedAvroSchema(getAvroSchema());
  }
}
