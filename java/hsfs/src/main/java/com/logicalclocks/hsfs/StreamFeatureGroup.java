/*
 *  Copyright (c) 2025. Hopsworks AB
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup<T> extends FeatureGroupBase<List<T>> {

  protected FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

  @Builder
  public StreamFeatureGroup(FeatureStoreBase featureStore, @NonNull String name, Integer version, String description,
                            TimeTravelFormat timeTravelFormat, List<String> primaryKeys, List<String> partitionKeys,
                            String hudiPrecombineKey, boolean onlineEnabled, List<Feature> features,
                            StatisticsConfig statisticsConfig, String onlineTopicName, String eventTime,
                            StorageConnector storageConnector, String path) {
    this();
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
    this.timeTravelFormat = timeTravelFormat;
    this.storageConnector = storageConnector;
    this.path = path;
  }

  public StreamFeatureGroup() {
    this.type = "streamFeatureGroupDTO";
  }

  // used for updates
  public StreamFeatureGroup(Integer id, String description, List<Feature> features) {
    this();
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public StreamFeatureGroup(FeatureStoreBase featureStore, int id) {
    this();
    this.featureStore = featureStore;
    this.id = id;
  }

  /**
   * Save the feature group metadata on Hopsworks.
   * This method is idempotent, if the feature group already exists the method does nothing.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save() throws FeatureStoreException, IOException {
    save(null, null);
  }

  /**
   * Save the feature group metadata on Hopsworks.
   * This method is idempotent, if the feature group already exists, the method does nothing
   *
   * @param writeOptions Options to provide to the materialization job
   * @param materializationJobConfiguration Resource configuration for the materialization job
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save(Map<String, String> writeOptions, JobConfiguration materializationJobConfiguration)
      throws FeatureStoreException, IOException {
    featureGroupEngine.save(this, partitionKeys, hudiPrecombineKey,
        writeOptions, materializationJobConfiguration);
  }

  @Override
  public List<T> insertStream(List<T> featureData) throws Exception {
    return insertStream(featureData, new HashMap<>());
  }

  @Override
  public List<T> insertStream(List<T> featureData, Map<String, String> writeOptions) throws Exception {
    return featureGroupEngine.insertStream(this, featureData, writeOptions);
  }

  /**
   * Select a subset of features of the feature group and return a query object. The query can be used to construct
   * joins of feature groups or create a feature view with a subset of features of the feature group.
   * @param features List of Feature meta data objects.
   * @return Query object.
   */
  public Query selectFeatures(List<Feature> features) {
    return new Query(this, features);
  }

  /**
   * Select a subset of features of the feature group and return a query object. The query can be used to construct
   * joins of feature groups or create a feature view with a subset of features of the feature group.
   * @param features List of Feature names.
   * @return Query object.
   */
  public Query select(List<String> features) {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  /**
   * Select all features of the feature group and return a query object. The query can be used to construct
   * joins of feature groups or create a feature view with a subset of features of the feature group.
   * @return Query object.
   */
  public Query selectAll() {
    return new Query(this, getFeatures());
  }

  /**
   * Select all features including primary key and event time feature of the feature group except provided `features`
   * and return a query object.
   * The query can be used to construct joins of feature groups or create a feature view with a subset of features of
   * the feature group.
   * @param features List of Feature meta data objects.
   * @return Query object.
   */
  public Query selectExceptFeatures(List<Feature> features) {
    List<String> exceptFeatures = features.stream().map(Feature::getName).collect(Collectors.toList());
    return selectExcept(exceptFeatures);
  }

  /**
   * Select all features including primary key and event time feature of the feature group except provided `features`
   * and return a query object.
   * The query can be used to construct joins of feature groups or create a feature view with a subset of features of
   * the feature group.
   * @param features List of Feature names.
   * @return Query object.
   */
  public Query selectExcept(List<String> features) {
    return new Query(this,
        getFeatures().stream().filter(f -> !features.contains(f.getName())).collect(Collectors.toList()));
  }
}
