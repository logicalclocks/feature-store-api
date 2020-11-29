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
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.Statistics;
import lombok.AllArgsConstructor;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup extends FeatureGroupBase {

  @Getter @Setter
  private Boolean onlineEnabled;

  @Getter @Setter
  private String type = "cachedFeaturegroupDTO";

  @Getter @Setter
  private TimeTravelFormat timeTravelFormat = TimeTravelFormat.HUDI;

  @Getter @Setter
  protected String location;

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
                      TimeTravelFormat timeTravelFormat, List<Feature> features, Boolean statisticsEnabled,
                      Boolean histograms, Boolean correlations, List<String> statisticColumns) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys;
    this.partitionKeys = partitionKeys;
    this.onlineEnabled = onlineEnabled;
    this.timeTravelFormat = timeTravelFormat != null ? timeTravelFormat : TimeTravelFormat.HUDI;
    this.features = features;
    this.statisticsEnabled = statisticsEnabled != null ? statisticsEnabled : true;
    this.histograms = histograms;
    this.correlations = correlations;
    this.statisticColumns = statisticColumns;
  }

  public FeatureGroup() {
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return selectAll().read(online);
  }

  public Dataset<Row> read(Map<String,String> readOptions) throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online, Map<String,String> readOptions) throws FeatureStoreException, IOException {
    return selectAll().read(online, readOptions);
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Dataset<Row> read(String wallclockTime) throws FeatureStoreException, IOException {
    return selectAll().asOf(wallclockTime).read(false, null);
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime
   * @param readOptions
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Dataset<Row> read(String wallclockTime, Map<String,String> readOptions)
      throws FeatureStoreException, IOException {
    return selectAll().asOf(wallclockTime).read(false, readOptions);
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime end date.
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, IOException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, null);
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime end date.
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime, Map<String,String> readOptions)
      throws FeatureStoreException, IOException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, readOptions);
  }


  public void show(int numRows) throws FeatureStoreException, IOException {
    show(numRows, false);
  }

  public void show(int numRows, boolean online) throws FeatureStoreException, IOException {
    read(online).show(numRows);
  }

  public void save(Dataset<Row> featureData) throws FeatureStoreException, IOException {
    save(featureData, null);
  }

  public void save(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureGroupEngine.saveFeatureGroup(this, featureData, primaryKeys, partitionKeys, writeOptions, null);
    if (statisticsEnabled) {
      statisticsEngine.computeStatistics(this, featureData);
    }
  }

  public void insert(Dataset<Row> featureData) throws IOException, FeatureStoreException {
    insert(featureData, null, false);
  }

  public void insert(Dataset<Row> featureData, Storage storage) throws IOException, FeatureStoreException {
    insert(featureData, storage, false, null, null, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite) throws IOException, FeatureStoreException {
    insert(featureData, null, overwrite);
  }

  public void insert(Dataset<Row> featureData, Map<String, Object> writeOptions)
      throws IOException, FeatureStoreException {
    if (this.getTimeTravelFormat() != TimeTravelFormat.DELTA) {
      insert(featureData, null, false, null, new HashMap<String, String>((Map) writeOptions),
          null);
    } else {
      insert(featureData, null, false, null, null, writeOptions);
    }
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException {
    insert(featureData, storage, overwrite, null,  null, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    insert(featureData, null, overwrite, null, writeOptions, null);
  }

  /**
   * Commit insert or upsert to time travel enabled Feature group.
   *
   * @param featureData dataframe to be committed.
   * @param operation commit operation type, INSERT or UPSERT.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Dataset<Row> featureData, ActionType operation)
      throws FeatureStoreException, IOException {
    insert(featureData, null, false, operation, null, null);
  }


  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite, ActionType operation,
                     Map<String, String> writeOptions, Map<String, Object> deltaCustomExpressions)
      throws FeatureStoreException, IOException {

    // operation is only valid for time travel enabled feature group
    if (operation != null && this.timeTravelFormat == TimeTravelFormat.NONE) {
      throw new IllegalArgumentException("operation argument is valid only for time travel enable feature groups");
    }

    if (operation == null && this.timeTravelFormat != TimeTravelFormat.NONE) {
      if (overwrite) {
        operation = ActionType.BULK_INSERT;
      } else {
        operation = ActionType.UPSERT;
      }
    }

    featureGroupEngine.saveDataframe(this, featureData, storage,
        overwrite ? SaveMode.Overwrite : SaveMode.Append, operation, writeOptions, deltaCustomExpressions);

    computeStatistics();
  }

  public void commitDeleteRecord(Dataset<Row> featureData)
      throws FeatureStoreException, IOException {

    // operation is only valid for time travel enabled feature group
    if (this.timeTravelFormat == TimeTravelFormat.NONE) {
      throw new FeatureStoreException("delete function is only valid for "
          + "time travel enabled feature group");
    }

    featureGroupEngine.commitDelete(this, featureData, null);
  }

  public void commitDeleteRecord(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {

    // operation is only valid for time travel enabled feature group
    if (this.timeTravelFormat == TimeTravelFormat.NONE) {
      throw new FeatureStoreException("delete function is only valid for "
          + "time travel enabled feature group");
    }

    featureGroupEngine.commitDelete(this, featureData, writeOptions);
  }

  /**
   * Return commit details.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<String, Map<String,String>>  commitDetails() throws IOException, FeatureStoreException {
    return featureGroupEngine.commitDetails(this, null);
  }

  /**
   * Return commit details.
   * @param limit number of commits to return.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<String, Map<String,String>>  commitDetails(Integer limit) throws IOException, FeatureStoreException {
    return featureGroupEngine.commitDetails(this, limit);
  }

  /**
   * Update the statistics configuration of the feature group.
   * Change the `statisticsEnabled`, `histograms`, `correlations` or `statisticColumns` attributes and persist
   * the changes by calling this method.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void updateStatisticsConfig() throws FeatureStoreException, IOException {
    featureGroupEngine.updateStatisticsConfig(this);
  }

  /**
   * Recompute the statistics for the feature group and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Statistics computeStatistics() throws FeatureStoreException, IOException {
    if (statisticsEnabled) {
      return statisticsEngine.computeStatistics(this, read());
    } else {
      LOGGER.info("StorageWarning: The statistics are not enabled of feature group `" + name + "`, with version `"
          + version + "`. No statistics computed.");
    }
    return null;
  }

  /**
   * Get the last statistics commit for the feature group.
   *
   * @return statistics object of latest commit
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    return statisticsEngine.getLast(this);
  }

  /**
   * Get the statistics of a specific commit time for the feature group.
   *
   * @param commitTime commit time in the format "YYYYMMDDhhmmss"
   * @return statistics object for the commit time
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Statistics getStatistics(String commitTime) throws FeatureStoreException, IOException {
    return statisticsEngine.get(this, commitTime);
  }
}
