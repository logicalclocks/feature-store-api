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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.metadata.Query;
import com.logicalclocks.hsfs.metadata.Statistics;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup {
  @Getter @Setter
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private Integer version;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private FeatureStore featureStore;

  @Getter @Setter
  private List<Feature> features;

  @Getter
  private Date created;

  @Getter
  private String creator;

  @Getter @Setter
  private Storage defaultStorage;

  @Getter @Setter
  private Boolean onlineEnabled;

  @Getter @Setter
  private String type = "cachedFeaturegroupDTO";

  @Getter @Setter
  @JsonProperty("descStatsEnabled")
  private Boolean statisticsEnabled;

  @Getter @Setter
  @JsonProperty("featHistEnabled")
  private Boolean histograms;

  @Getter @Setter
  @JsonProperty("featCorrEnabled")
  private Boolean correlations;

  @Getter @Setter
  private List<String> statisticColumns;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> primaryKeys;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  private FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  private StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Builder
  public FeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                      List<String> primaryKeys, List<String> partitionKeys, boolean onlineEnabled,
                      Storage defaultStorage, List<Feature> features, Boolean statisticsEnabled, Boolean histograms,
                      Boolean correlations, List<String> statisticColumns)
      throws FeatureStoreException {

    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys;
    this.partitionKeys = partitionKeys;
    this.onlineEnabled = onlineEnabled;
    this.defaultStorage = defaultStorage != null ? defaultStorage : Storage.OFFLINE;
    this.features = features;
    this.statisticsEnabled = statisticsEnabled;
    this.histograms = histograms;
    this.correlations = correlations;
    this.statisticColumns = statisticColumns;
  }

  public FeatureGroup() {
  }

  public Query selectFeatures(List<Feature> features) throws FeatureStoreException, IOException {
    return new Query(this, features);
  }

  public Query selectAll() throws FeatureStoreException, IOException {
    return new Query(this, getFeatures());
  }

  public Query select(List<String> features) throws FeatureStoreException, IOException {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList  = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(this.defaultStorage);
  }

  public Dataset<Row> read(Storage storage) throws FeatureStoreException, IOException {
    return selectAll().read(storage);
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    show(numRows, defaultStorage);
  }

  public void show(int numRows, Storage storage) throws FeatureStoreException, IOException {
    read(storage).show(numRows);
  }

  public void save(Dataset<Row> featureData) throws FeatureStoreException, IOException {
    save(featureData, null);
  }

  public void save(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureGroupEngine.saveFeatureGroup(this, featureData, primaryKeys, partitionKeys, defaultStorage, writeOptions);
    if (statisticsEnabled) {
      statisticsEngine.computeStatistics(this, featureData);
    }
  }

  public void insert(Dataset<Row> featureData, Storage storage) throws IOException, FeatureStoreException {
    insert(featureData, storage, false, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite) throws IOException, FeatureStoreException {
    insert(featureData, overwrite, null);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException {
    insert(featureData, storage, overwrite, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    insert(featureData, defaultStorage, overwrite, writeOptions);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureGroupEngine.saveDataframe(this, featureData, storage,
        overwrite ? SaveMode.Overwrite : SaveMode.Append, writeOptions);
    computeStatistics();
  }

  public void delete() throws FeatureStoreException, IOException {
    featureGroupEngine.delete(this);
  }

  public Statistics computeStatistics() throws FeatureStoreException, IOException {
    if (statisticsEnabled) {
      if (defaultStorage == Storage.ALL || defaultStorage == Storage.OFFLINE) {
        return statisticsEngine.computeStatistics(this, read(Storage.OFFLINE));
      } else {
        LOGGER.info("StorageWarning: The default storage of feature group `" + name + "`, with version `" + version
            + "`, is `" + defaultStorage + "`. Statistics are only computed for default storage `offline and `all`.");
      }
    }
    return null;
  }

  /**
   * Add a tag without value to the feature group.
   *
   * @param name name of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name) throws FeatureStoreException, IOException {
    addTag(name, null);
  }

  /**
   * Add name/value tag to the feature group.
   *
   * @param name name of the tag
   * @param value value of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name, String value) throws FeatureStoreException, IOException {
    featureGroupEngine.addTag(this, name, value);
  }

  /**
   * Get all tags of the feature group.
   *
   * @return map of all tags from name to value
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, String> getTag() throws FeatureStoreException, IOException {
    return getTag(null);
  }

  /**
   * Get a single tag value of the feature group.
   *
   * @param name name of tha tag
   * @return string value of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, String> getTag(String name) throws FeatureStoreException, IOException {
    return featureGroupEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the feature group.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    featureGroupEngine.deleteTag(this, name);
  }
}
