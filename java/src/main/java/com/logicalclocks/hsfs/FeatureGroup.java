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
import com.logicalclocks.hsfs.util.Constants;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup extends FeatureGroupBase {

  @Getter @Setter
  private Storage defaultStorage;

  @Getter @Setter
  private Boolean onlineEnabled;

  @Getter @Setter
  private String type = "cachedFeaturegroupDTO";

  // TODO (davit): this must be Getter only.
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
                      TimeTravelFormat timeTravelFormat, Storage defaultStorage, List<Feature> features,
                      Boolean statisticsEnabled, Boolean histograms, Boolean correlations,
                      List<String> statisticColumns) {

    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys;
    this.partitionKeys = partitionKeys;
    this.onlineEnabled = onlineEnabled;
    this.timeTravelFormat = timeTravelFormat != null ? timeTravelFormat : TimeTravelFormat.HUDI;
    this.defaultStorage = defaultStorage != null ? defaultStorage : Storage.OFFLINE;
    this.features = features;
    this.statisticsEnabled = statisticsEnabled != null ? statisticsEnabled : true;
    this.histograms = histograms;
    this.correlations = correlations;
    this.statisticColumns = statisticColumns;
  }

  public FeatureGroup() {
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(this.defaultStorage);
  }

  public Dataset<Row> read(Storage storage) throws FeatureStoreException, IOException {
    return selectAll().read(storage);
  }

  // Time travel queries
  public Dataset<Row> read(String wallclocktime) throws FeatureStoreException, IOException {
    return selectAll().asOf(wallclocktime).read(this.defaultStorage);
  }

  public Dataset<Row> readChanges(String startWallclocktime, String endWallclocktime)
      throws FeatureStoreException, IOException {
    return selectAll().pullChanges(startWallclocktime, endWallclocktime).read(this.defaultStorage);
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

    featureGroupEngine.saveFeatureGroup(this, featureData, primaryKeys, partitionKeys,
            defaultStorage, writeOptions);

    if (statisticsEnabled) {
      statisticsEngine.computeStatistics(this, featureData);
    }
  }

  public void insert(Dataset<Row> featureData, Storage storage)
      throws IOException, FeatureStoreException {
    insert(featureData, storage, false, null, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite)
      throws IOException, FeatureStoreException {
    insert(featureData, overwrite, null);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException {
    insert(featureData, storage, overwrite, null,  null);
  }

  // time-travel enabled insert with upsert op
  public void insert(Dataset<Row> featureData, String operation)
      throws FeatureStoreException, IOException {

    List<String> supportedOps = Arrays.asList(Constants.HUDI_UPSERT, Constants.HUDI_INSERT);

    if (!supportedOps.stream().anyMatch(x -> x.equalsIgnoreCase(operation))) {
      throw new IllegalArgumentException("For inserting in time travel enabled feature group only operations "
              + Constants.HUDI_UPSERT + " and " + Constants.HUDI_INSERT + " are supported");
    }

    insert(featureData, defaultStorage, false, operation, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    insert(featureData, defaultStorage, overwrite, null, writeOptions);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite, String operation,
                     Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {

    // operation is only valid for time travel enabled feature group
    if (this.timeTravelFormat == TimeTravelFormat.NONE && operation != null) {
      throw new IllegalArgumentException("Argument operation is only valid for "
              + "time travel enabled feature group");
    }

    featureGroupEngine.saveDataframe(this, featureData, storage,
        overwrite ? SaveMode.Overwrite : SaveMode.Append, operation,
            writeOptions);

    computeStatistics();
  }


  public void delete(Dataset<Row> featureData)
      throws FeatureStoreException, IOException {

    // operation is only valid for time travel enabled feature group
    if (this.timeTravelFormat == TimeTravelFormat.NONE) {
      throw new FeatureStoreException("delete function is only valid for "
          + "time travel enabled feature group");
    }

    featureGroupEngine.commitDelete(this, featureData);
  }

  public FeatureGroupCommit[] commitDetails(Integer limit) throws IOException, FeatureStoreException {
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
