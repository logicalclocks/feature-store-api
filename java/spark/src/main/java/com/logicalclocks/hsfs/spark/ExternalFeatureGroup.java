/*
 *  Copyright (c) 2020-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.spark.constructor.Query;
import com.logicalclocks.hsfs.spark.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.spark.engine.StatisticsEngine;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.ExternalDataFormat;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.JobConfiguration;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.Storage;

import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.engine.CodeEngine;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.OnDemandOptions;
import com.logicalclocks.hsfs.metadata.Statistics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalFeatureGroup extends FeatureGroupBase<Dataset<Row>> {

  @Getter
  @Setter
  private StorageConnector storageConnector;

  @Getter
  @Setter
  private String query;

  @Getter
  @Setter
  private ExternalDataFormat dataFormat;

  @Getter
  @Setter
  private String path;

  @Getter
  @Setter
  private List<OnDemandOptions> options;


  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

  private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);

  @Builder
  public ExternalFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String query,
                              ExternalDataFormat dataFormat, String path, Map<String, String> options,
                              @NonNull StorageConnector storageConnector, String description, List<String> primaryKeys,
                              List<Feature> features, StatisticsConfig statisticsConfig, String eventTime,
                              boolean onlineEnabled, String onlineTopicName, String topicName,
                              String notificationTopicName) {
    this();
    this.timeTravelFormat = null;
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.query = query;
    this.dataFormat = dataFormat;
    this.path = path;
    this.options = options != null ? options.entrySet().stream()
        .map(e -> new OnDemandOptions(e.getKey(), e.getValue()))
        .collect(Collectors.toList())
        : null;
    this.description = description;
    this.primaryKeys = primaryKeys != null
        ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.storageConnector = storageConnector;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.eventTime = eventTime;
    this.onlineEnabled = onlineEnabled;
    this.onlineTopicName = onlineTopicName;
    this.topicName = topicName;
    this.notificationTopicName = notificationTopicName;
  }

  public ExternalFeatureGroup() {
    this.type = "onDemandFeaturegroupDTO";
  }

  public ExternalFeatureGroup(FeatureStore featureStore, int id) {
    this();
    this.featureStore = featureStore;
    this.id = id;
  }

  public void save() throws FeatureStoreException, IOException {
    featureGroupEngine.saveExternalFeatureGroup(this);
    codeEngine.saveCode(this);
    if (statisticsConfig.getEnabled()) {
      statisticsEngine.computeStatistics(this, read(), null);
    }
  }

  @Override
  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return selectAll().read();
  }

  @Override
  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return selectAll().read(online);
  }

  @Override
  public Dataset<Row> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return selectAll().read(false, readOptions);
  }

  @Override
  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return selectAll().read(online, readOptions);
  }

  @Override
  public Dataset<Row> read(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public Dataset<Row> read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public QueryBase asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public QueryBase asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public void show(int numRows) throws FeatureStoreException, IOException {
    read().show(numRows);
  }

  @Override
  public void show(int numRows, boolean online) throws FeatureStoreException, IOException {
    read(true).show(numRows);
  }

  @Override
  public void insert(Dataset<Row> featureData, Storage storage)
      throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(Dataset<Row> featureData, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Dataset<Row> featureData, HudiOperationType operation)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite, HudiOperationType operation,
                     Map<String, String> writeOptions) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Dataset<Row> featureData, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions,
                     JobConfiguration jobConfiguration) throws FeatureStoreException, IOException, ParseException {

  }

  /**
   * Incrementally insert data to the online storage of an external feature group. The feature group has to be online
   * enabled to perform this operation.
   * The `features` dataframe can be a Spark DataFrame or RDD.
   * If statistics are enabled, statistics are recomputed for the entire feature group.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        ExternalFeatureGroup fg = fs.getExternalFeatureGroup("electricity_prices", 1);
   *        // insert data
   *        fg.insert(featureData, writeOptions);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile schema.
   */
  @Override
  public void insert(Dataset<Row> featureData)
      throws FeatureStoreException, IOException {

    featureGroupEngine.insert(this, featureData, null);

    codeEngine.saveCode(this);
    computeStatistics();
  }

  /**
   * Incrementally insert data to the online storage of an external feature group. The feature group has to be online
   * enabled to perform this operation.
   * The `features` dataframe can be a Spark DataFrame or RDD.
   * If statistics are enabled, statistics are recomputed for the entire feature group.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        ExternalFeatureGroup fg = fs.getExternalFeatureGroup("electricity_prices", 1);
   *        // Define additional write options (for example for Spark)
   *        Map<String, String> writeOptions = = new HashMap<String, String>();
   *        // insert data
   *        fg.insert(featureData, writeOptions);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param writeOptions Additional write options as key-value pairs.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile schema.
   */
  @Override
  public void insert(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {

    featureGroupEngine.insert(this, featureData, writeOptions);

    codeEngine.saveCode(this);
    computeStatistics();
  }

  @Override
  public void commitDeleteRecord(Dataset<Row> featureData) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void commitDeleteRecord(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return null;
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

  @Override
  public Object insertStream(Dataset<Row> featureData) throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName) throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, Map<String, String> writeOptions) throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, Map<String, String> writeOptions)
      throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, String outputMode) throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, String outputMode, String checkpointLocation)
      throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, String outputMode, boolean awaitTermination,
                             Long timeout) throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, String outputMode, boolean awaitTermination,
                             Long timeout, String checkpointLocation) throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, String outputMode, boolean awaitTermination,
                             Long timeout, String checkpointLocation, Map<String, String> writeOptions)
      throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, String outputMode, boolean awaitTermination,
                             String checkpointLocation) throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, String outputMode, boolean awaitTermination,
                             Long timeout, String checkpointLocation, Map<String, String> writeOptions,
                             JobConfiguration jobConfiguration) throws Exception {
    throw new UnsupportedOperationException("insertStream method is not supported in ExternalFeatureGroup");
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

  @Override
  public Statistics computeStatistics(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    return null;
  }
}
