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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.CodeEngine;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.metadata.Statistics;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.Schema;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  private Boolean onlineEnabled;

  @Getter
  @Setter
  private String type = "cachedFeaturegroupDTO";

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

  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Builder
  public FeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                      List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                      boolean onlineEnabled, TimeTravelFormat timeTravelFormat, List<Feature> features,
                      StatisticsConfig statisticsConfig, String onlineTopicName, String eventTime) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
        ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
        ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = timeTravelFormat == TimeTravelFormat.HUDI && hudiPrecombineKey != null
        ? hudiPrecombineKey.toLowerCase() : null;
    this.onlineEnabled = onlineEnabled;
    this.timeTravelFormat = timeTravelFormat != null ? timeTravelFormat : TimeTravelFormat.HUDI;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.onlineTopicName = onlineTopicName;
    this.eventTime = eventTime;
  }

  public FeatureGroup() {
  }

  // used for updates
  public FeatureGroup(Integer id, String description, List<Feature> features) {
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public FeatureGroup(FeatureStore featureStore, int id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return (Dataset<Row>) selectAll().read(online);
  }

  public Dataset<Row> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return read(false, readOptions);
  }

  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return (Dataset<Row>) selectAll().read(online, readOptions);
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   */
  public Dataset<Row> read(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    return (Dataset<Row>) selectAll().asOf(wallclockTime).read(false, null);
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime
   * @param readOptions
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   */
  public Dataset<Row> read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return (Dataset<Row>) selectAll().asOf(wallclockTime).read(false, readOptions);
  }

  /**
   * `readChanges` method is deprecated. Use `asOf(wallclockEndTime, wallclockStartTime).read(readOptions)` instead.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   *
   * @deprecated
   */
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, IOException, ParseException {
    return (Dataset<Row>) selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, null);
  }

  /**
   * `readChanges` method is deprecated. Use `asOf(wallclockEndTime, wallclockStartTime).read(readOptions)` instead.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   *
   * @deprecated
   */
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return (Dataset<Row>) selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, readOptions);
  }

  /**
   * Get Query object to retrieve all features of the group at a point in the past.
   * This method selects all features in the feature group and returns a Query object
   * at the specified point in time. This can then either be read into a Dataframe
   * or used further to perform joins or construct a training dataset.
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, or `%Y%m%d%H%M%S`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException
   * @throws ParseException
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
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, or `%Y%m%d%H%M%S`.
   * @param excludeUntil Datetime string. The String should be formatted in one of the
    *     following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, or `%Y%m%d%H%M%S`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException
   * @throws ParseException
   */
  public Query asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    return selectAll().asOf(wallclockTime, excludeUntil);
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    show(numRows, false);
  }

  public void show(int numRows, boolean online) throws FeatureStoreException, IOException {
    read(online).show(numRows);
  }

  /*
   * @deprecated
   * Save method is deprecated. Use insert method instead.
   */
  @Deprecated
  public void save(Dataset<Row> featureData) throws FeatureStoreException, IOException, ParseException {
    save(featureData, null);
  }

  /*
   * @deprecated
   * Save method is deprecated. Use insert method instead.
   */
  @Deprecated
  public void save(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.save(this, featureData, partitionKeys, hudiPrecombineKey,
        writeOptions);
    codeEngine.saveCode(this);
    if (statisticsConfig.getEnabled()) {
      statisticsEngine.computeStatistics(this, featureData, null);
    }
  }

  public void insert(Dataset<Row> featureData) throws IOException, FeatureStoreException, ParseException {
    insert(featureData, null, false);
  }

  public void insert(Dataset<Row> featureData,  Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, false, null, writeOptions);
  }

  public void insert(Dataset<Row> featureData, Storage storage)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, storage, false, null, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, null, overwrite);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, storage, overwrite, null, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, overwrite, null, writeOptions);
  }

  /**
   * Commit insert or upsert to time travel enabled Feature group.
   *
   * @param featureData dataframe to be committed.
   * @param operation   commit operation type, INSERT or UPSERT.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Dataset<Row> featureData, HudiOperationType operation)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, false, operation, null);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite, HudiOperationType operation,
                     Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {

    // operation is only valid for time travel enabled feature group
    if (operation != null && this.timeTravelFormat == TimeTravelFormat.NONE) {
      throw new IllegalArgumentException("operation argument is valid only for time travel enable feature groups");
    }

    if (operation == null && this.timeTravelFormat == TimeTravelFormat.HUDI) {
      if (overwrite) {
        operation = HudiOperationType.BULK_INSERT;
      } else {
        operation = HudiOperationType.UPSERT;
      }
    }

    featureGroupEngine.insert(this, featureData, storage, operation,
        overwrite ? SaveMode.Overwrite : SaveMode.Append, partitionKeys, hudiPrecombineKey, writeOptions);

    codeEngine.saveCode(this);
    computeStatistics();
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  public StreamingQuery insertStream(Dataset<Row> featureData)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, null);
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, "append");
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, outputMode, false, null, null, null);
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, null, null);
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, String checkpointLocation)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, outputMode, awaitTermination, null, checkpointLocation, null);
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout, String checkpointLocation,
                                     Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException, ParseException {
    if (!featureData.isStreaming()) {
      throw new FeatureStoreException(
          "Features have to be a streaming type spark dataframe. Use `insert()` method instead.");
    }
    LOGGER.info("StatisticsWarning: Stream ingestion for feature group `" + name + "`, with version `" + version
        + "` will not compute statistics.");

    return featureGroupEngine.insertStream(this, featureData, queryName, outputMode, awaitTermination,
        timeout, checkpointLocation, partitionKeys, hudiPrecombineKey, writeOptions);
  }

  public void commitDeleteRecord(Dataset<Row> featureData)
      throws FeatureStoreException, IOException, ParseException {
    utils.commitDelete(this, featureData, null);
  }

  public void commitDeleteRecord(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    utils.commitDelete(this, featureData, writeOptions);
  }

  /**
   * Return commit details.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetails(this, null);
  }

  /**
   * Return commit details.
   *
   * @param limit number of commits to return.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetails(this, limit);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetailsByWallclockTime(this, wallclockTime, null);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @param limit number of commits to return.
   * @throws FeatureStoreException
   * @throws IOException
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

  /**
   * Recompute the statistics for the feature group and save them to the feature store.
   *
   * @param wallclockTime number of commits to return.
   * @return statistics object of computed statistics
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Statistics computeStatistics(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    if (statisticsConfig.getEnabled()) {
      Map<Long, Map<String, String>> latestCommitMetaData =
          utils.commitDetailsByWallclockTime(this, wallclockTime, 1);
      Dataset<Row> featureData = (Dataset<Row>) selectAll().asOf(wallclockTime).read(false, null);
      Long commitId = (Long) latestCommitMetaData.keySet().toArray()[0];
      return statisticsEngine.computeStatistics(this, featureData, commitId);
    } else {
      LOGGER.info("StorageWarning: The statistics are not enabled of feature group `" + name + "`, with version `"
          + version + "`. No statistics computed.");
    }
    return null;
  }
}
