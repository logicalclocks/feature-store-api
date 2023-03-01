/*
 *  Copyright (c) 2020-2022. Hopsworks AB
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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.base.DeltaStreamerJobConf;
import com.logicalclocks.base.EntityEndpointType;
import com.logicalclocks.base.Feature;

import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.HudiOperationType;
import com.logicalclocks.base.StatisticsConfig;
import com.logicalclocks.base.Storage;
import com.logicalclocks.base.TimeTravelFormat;
import com.logicalclocks.base.engine.CodeEngine;
import com.logicalclocks.base.FeatureGroupBase;
import com.logicalclocks.base.metadata.Statistics;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
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
import org.apache.avro.Schema;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
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
  private List<String> statisticColumns;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  @JsonIgnore
  // This is only used in the client. In the server they are aggregated in the `features` field
  private String hudiPrecombineKey;

  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  protected StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);

  @Builder
  public FeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version,
                      String description,
                      List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                      boolean onlineEnabled, TimeTravelFormat timeTravelFormat, List<Feature> features,
                      StatisticsConfig statisticsConfig, String onlineTopicName, String eventTime) {
    this();
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
    this.type = "cachedFeaturegroupDTO";
  }

  public FeatureGroup(FeatureStore featureStore, Integer id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  // used for updates
  public FeatureGroup(Integer id, String description, List<Feature> features) {
    this();
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public FeatureGroup(FeatureStore featureStore, int id) {
    this();
    this.featureStore = featureStore;
    this.id = id;
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return selectAll().read(online);
  }

  public Dataset<Row> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return read(false, readOptions);
  }

  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return selectAll().read(online, readOptions);
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime point in time
   * @return Spark DataFrame containing feature data.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public Dataset<Row> read(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime).read(false, null);
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime point in time
   * @param readOptions Additional read options as key-value pairs, defaults to empty Map.
   * @return Spark DataFrame containing feature data.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public Dataset<Row> read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime).read(false, readOptions);
  }

  /**
   * `readChanges` method is deprecated. Use `asOf(wallclockEndTime, wallclockStartTime).read(readOptions)` instead.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return Spark DataFrame containing feature data.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   *
   * @deprecated
   */
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, null);
  }

  /**
   * `readChanges` method is deprecated. Use `asOf(wallclockEndTime, wallclockStartTime).read(readOptions)` instead.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime end date.
   * @param readOptions Additional write options as key-value pairs, defaults to empty Map.
   * @return Spark DataFrame containing feature data.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   *
   * @deprecated
   */
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, readOptions);
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
   * @throws FeatureStoreException FeatureStoreException
   * @throws ParseException ParseException
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
   * @throws FeatureStoreException FeatureStoreException
   * @throws ParseException ParseException
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
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
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
   * @param featureData Spark dataframe containing feature data
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
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
   * @param featureData Spark dataframe containing feature data
   * @param queryName name of spark StreamingQuery
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
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
   * @param featureData Spark dataframe containing feature data
   * @param queryName name of spark StreamingQuery
   * @param outputMode outputMode
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
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
   * @param featureData Spark dataframe containing feature data
   * @param queryName name of spark StreamingQuery
   * @param outputMode outputMode
   * @param awaitTermination whether or not to wait for query Termination
   * @param timeout timeout
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
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
   * @param featureData Spark dataframe containing feature data
   * @param queryName name of spark StreamingQuery
   * @param outputMode outputMode
   * @param awaitTermination whether or not to wait for query Termination
   * @param checkpointLocation path to checkpoint location directory
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
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
   * @param featureData Spark dataframe containing feature data
   * @param queryName name of spark StreamingQuery
   * @param outputMode outputMode
   * @param awaitTermination whether or not to wait for query Termination
   * @param timeout timeout
   * @param checkpointLocation path to checkpoint location directory
   * @param writeOptions Additional write options as key-value pairs, defaults to empty Map.
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
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
    featureGroupEngine.commitDelete(this, featureData, null);
  }

  public void commitDeleteRecord(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.commitDelete(this, featureData, writeOptions);
  }

  /**
   * Return commit details.
   *
   * @return commit details.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, null);
  }

  /**
   * Return commit details.
   *
   * @param limit number of commits to return.
   * @return commit details.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, limit);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @return commit details.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, null);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @param limit number of commits to return.
   * @return commit details.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, limit);
  }

  public Query selectFeatures(List<Feature> features) {
    return new Query(this, features);
  }

  public Query select(List<String> features) {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  public Query selectAll() {
    return new Query(this, getFeatures());
  }

  public Query selectExceptFeatures(List<Feature> features) {
    List<String> exceptFeatures = features.stream().map(Feature::getName).collect(Collectors.toList());
    return selectExcept(exceptFeatures);
  }

  public Query selectExcept(List<String> features) {
    return new Query(this,
        getFeatures().stream().filter(f -> !features.contains(f.getName())).collect(Collectors.toList()));
  }

  @Override
  public void updateFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.appendFeatures(this, features, this.getClass());
  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.appendFeatures(this, Collections.singletonList(feature), this.getClass());
  }

  @Override
  public void appendFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.appendFeatures(this, new ArrayList<>(features), this.getClass());
  }

  @Override
  public void appendFeatures(Feature features) throws FeatureStoreException, IOException, ParseException {
    List<Feature> featureList = new ArrayList<>();
    featureList.add(features);
    featureGroupEngine.appendFeatures(this, featureList, this.getClass());
  }

  @Override
  public Statistics computeStatistics() throws FeatureStoreException, IOException {
    if (statisticsConfig.getEnabled()) {
      return statisticsEngine.computeStatistics(this, read(), null);
    } else {
      LOGGER.info("StorageWarning: The statistics are not enabled of feature group `" + name + "`, with version `"
          + version + "`. No statistics computed.");
    }
    return null;
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
          featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, 1);
      Dataset<Row> featureData = selectAll().asOf(wallclockTime).read(false, null);
      Long commitId = (Long) latestCommitMetaData.keySet().toArray()[0];
      return statisticsEngine.computeStatistics(this, featureData, commitId);
    } else {
      LOGGER.info("StorageWarning: The statistics are not enabled of feature group `" + name + "`, with version `"
          + version + "`. No statistics computed.");
    }
    return null;
  }

  @JsonIgnore
  public List<String> getComplexFeatures() {
    return utils.getComplexFeatures(features);
  }

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    return getSubject().getSchema();
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

  @Override
  public void setDeltaStreamerJobConf(DeltaStreamerJobConf deltaStreamerJobConf)
      throws FeatureStoreException, IOException {
  }
}
