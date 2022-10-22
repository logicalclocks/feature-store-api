/*
 *  Copyright (c) 2022-2022. Hopsworks AB
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
import com.logicalclocks.generic.DeltaStreamerJobConf;
import com.logicalclocks.generic.EntityEndpointType;
import com.logicalclocks.generic.Feature;
import com.logicalclocks.generic.FeatureStoreException;
import com.logicalclocks.generic.JobConfiguration;
import com.logicalclocks.generic.SaveMode;
import com.logicalclocks.generic.TimeTravelFormat;
import com.logicalclocks.generic.engine.CodeEngine;
import com.logicalclocks.generic.engine.FeatureGroupUtils;
import com.logicalclocks.generic.metadata.FeatureGroupBase;
import com.logicalclocks.generic.metadata.Statistics;

import com.logicalclocks.hsfs.constructor.Query;

import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.engine.StatisticsEngine;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  protected FeatureStore featureStore;

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

  @JsonIgnore
  private String avroSchema;

  @Getter(onMethod = @__(@Override))
  @Setter(onMethod = @__(@Override))
  private String onlineTopicName;

  @Setter
  private DeltaStreamerJobConf deltaStreamerJobConf;

  @Getter(onMethod = @__(@Override))
  @Setter
  private TimeTravelFormat timeTravelFormat = TimeTravelFormat.HUDI;

  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);
  private FeatureGroupUtils utils = new FeatureGroupUtils();

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

  public StreamFeatureGroup() {
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
   * Reads Feature group data.
   *
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   */
  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return read(online, null);
  }

  @Override
  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return selectAll().read(online, readOptions);
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @deprecated
   *   `readChanges` method is deprecated. Use `asOf(wallclockEndTime, wallclockStartTime).read()` instead.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   */
  @Deprecated
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, null);
  }

  /*
   * @deprecated
   * `readChanges` method is deprecated. Use `asOf(wallclockEndTime, wallclockStartTime).read(readOptions)` instead.
   */
  @Deprecated
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

  /*
   * @deprecated
   * `save` method is deprecated and in the next release it will be  replaced by `insert` and `insertStream` methods.
   */
  @Deprecated
  public void save(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.save(this, featureData, partitionKeys, hudiPrecombineKey, writeOptions, null);
    codeEngine.saveCode(this);
  }

  /*
   * @deprecated
   * In the next release save method will be  replaced by insert method.
   */
  @Deprecated
  public void save(Dataset<Row> featureData, Map<String, String> writeOptions, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.save(this, featureData, partitionKeys, hudiPrecombineKey, writeOptions,
        jobConfiguration);
    codeEngine.saveCode(this);
  }

  public void insert(Dataset<Row> featureData) throws FeatureStoreException, IOException, ParseException {
    insert(featureData, SaveMode.APPEND, null, null);
  }

  public void insert(Dataset<Row> featureData, Map<String, String> writeOptions) throws FeatureStoreException,
      IOException, ParseException {
    insert(featureData, SaveMode.APPEND, writeOptions, null);
  }

  public void insert(Dataset<Row>  featureData, JobConfiguration jobConfiguration) throws FeatureStoreException,
      IOException, ParseException {
    insert(featureData, SaveMode.APPEND, null, jobConfiguration);
  }

  public void insert(Dataset<Row> featureData, SaveMode saveMode,
                         Map<String, String> writeOptions) throws FeatureStoreException, IOException, ParseException {
    insert(featureData, saveMode, writeOptions, null);
  }

  public void insert(Dataset<Row>  featureData, SaveMode saveMode,
                         JobConfiguration jobConfiguration) throws FeatureStoreException, IOException, ParseException {
    insert(featureData, saveMode, null, jobConfiguration);
  }

  public void insert(Dataset<Row>  featureData, SaveMode saveMode,
                         Map<String, String> writeOptions, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

    featureGroupEngine.insert(this, featureData, saveMode,  partitionKeys,
        hudiPrecombineKey, writeOptions, jobConfiguration);
    codeEngine.saveCode(this);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData) {
    return insertStream(featureData, null, "append", false, null, null, null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName) {
    return insertStream(featureData, queryName, null,false, null, null, null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, Map<String, String> writeOptions) {
    return insertStream(featureData, null, null, false, null, null, writeOptions);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, Map<String, String> writeOptions) {
    return insertStream(featureData, queryName, "append", false, null, null, writeOptions);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode) {
    return insertStream(featureData, queryName, outputMode, false, null, null, null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     String checkpointLocation) {
    return insertStream(featureData, queryName, outputMode, false, null, checkpointLocation, null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout) {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, null, null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout, String checkpointLocation) {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, checkpointLocation,
        null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout,  String checkpointLocation,
                                     Map<String, String> writeOptions) {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, checkpointLocation, writeOptions,
        null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout,  String checkpointLocation,
                                     Map<String, String> writeOptions, JobConfiguration jobConfiguration) {
    return featureGroupEngine.insertStream(this, featureData, queryName, outputMode,
        awaitTermination, timeout, checkpointLocation,  partitionKeys, hudiPrecombineKey, writeOptions,
        jobConfiguration);
  }

  public void commitDeleteRecord(Dataset<Row>  featureData)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.commitDelete(this, featureData, null);
  }

  public void commitDeleteRecord(Dataset<Row>  featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.commitDelete(this, featureData, writeOptions);
  }

  /**
   * Return commit details.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, null);
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
    return featureGroupEngine.commitDetails(this, limit);

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
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, null);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @param limit number of commits to return.
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, limit);
  }

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    if (avroSchema == null) {
      avroSchema = utils.getAvroSchema(this, this.featureStore);
    }
    return avroSchema;
  }

  @JsonIgnore
  @Override
  public List<String> getComplexFeatures() {
    return utils.getComplexFeatures(features);
  }

  @JsonIgnore
  @Override
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    return utils.getFeatureAvroSchema(featureName, utils.getDeserializedAvroSchema(getAvroSchema()));
  }

  @JsonIgnore
  @Override
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getEncodedAvroSchema(getDeserializedAvroSchema(), utils.getComplexFeatures(features));
  }

  @JsonIgnore
  @Override
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getDeserializedAvroSchema(getAvroSchema());
  }

  @Override
  public Query selectFeatures(List<Feature> features) {
    return new Query(this, features);
  }

  @Override
  public Query select(List<String> features) {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  @Override
  public Query selectAll() {
    return new Query(this, getFeatures());
  }

  @Override
  public Query selectExceptFeatures(List<Feature> features) {
    List<String> exceptFeatures = features.stream().map(Feature::getName).collect(Collectors.toList());
    return selectExcept(exceptFeatures);
  }

  @Override
  public Query selectExcept(List<String> features) {
    return new Query(this,
        getFeatures().stream().filter(f -> !features.contains(f.getName())).collect(Collectors.toList()));
  }

  /**
   * Update the metadata of multiple features.
   * Currently only feature description updates are supported.
   *
   * @param features
   * @throws FeatureStoreException
   * @throws IOException
   */
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
}
