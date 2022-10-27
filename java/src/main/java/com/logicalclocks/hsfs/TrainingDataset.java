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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.logicalclocks.base.EntityEndpointType;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.Split;
import com.logicalclocks.base.Storage;
import com.logicalclocks.base.TrainingDatasetBase;
import com.logicalclocks.base.TrainingDatasetFeature;
import com.logicalclocks.base.TrainingDatasetType;
import com.logicalclocks.base.constructor.Filter;
import com.logicalclocks.base.constructor.FilterLogic;
import com.logicalclocks.base.engine.CodeEngine;
import com.logicalclocks.base.engine.FeatureGroupUtils;
import com.logicalclocks.base.metadata.Statistics;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.engine.TrainingDatasetEngine;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class TrainingDataset extends TrainingDatasetBase {

  @Getter
  @Setter
  private Integer id;

  @Getter
  @Setter
  private String name;

  @Getter
  @Setter
  private Integer version;

  @Getter
  @Setter
  private String description;

  @Getter
  @Setter
  private DataFormat dataFormat;

  @Getter
  @Setter
  private Boolean coalesce;

  @Getter
  @Setter
  private TrainingDatasetType trainingDatasetType = TrainingDatasetType.HOPSFS_TRAINING_DATASET;

  @Getter
  @Setter
  private List<TrainingDatasetFeature> features;

  @Getter
  @Setter
  @JsonIgnore
  private FeatureStore featureStore;

  @Getter
  @Setter
  private StorageConnector storageConnector;

  @Getter
  @Setter
  private String location;

  @Getter
  @Setter
  private Long seed;

  @Getter
  @Setter
  private List<Split> splits;

  @Getter
  @Setter
  private String trainSplit;

  @Getter
  @Setter
  private StatisticsConfig statisticsConfig = new StatisticsConfig();

  @Getter
  @Setter
  @JsonProperty("queryDTO")
  private Query queryInt;

  @JsonIgnore
  private List<String> label;

  @Getter
  @Setter
  private Date eventStartTime;

  @Getter
  @Setter
  private Date eventEndTime;

  @Getter
  @Setter
  private FilterLogic extraFilter;

  @Getter
  @Setter
  private String type = "trainingDatasetDTO";

  private TrainingDatasetEngine trainingDatasetEngine = new TrainingDatasetEngine();
  private StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.TRAINING_DATASET);
  private CodeEngine codeEngine = new CodeEngine(EntityEndpointType.TRAINING_DATASET);

  @Builder
  public TrainingDataset(@NonNull String name, Integer version, String description, DataFormat dataFormat,
                         Boolean coalesce, StorageConnector storageConnector, String location, List<Split> splits,
                         String trainSplit,
                         Long seed, FeatureStore featureStore, StatisticsConfig statisticsConfig, List<String> label,
                         String eventStartTime, String eventEndTime, TrainingDatasetType trainingDatasetType,
                         Float validationSize, Float testSize, String trainStart, String trainEnd,
                         String validationStart,
                         String validationEnd, String testStart, String testEnd, Integer timeSplitSize,
                         FilterLogic extraFilterLogic,
                         Filter extraFilter)
      throws FeatureStoreException, ParseException {
    this.name = name;
    this.version = version;
    this.description = description;
    this.dataFormat = dataFormat != null ? dataFormat : DataFormat.CSV;
    this.coalesce = coalesce != null ? coalesce : false;
    this.location = location;
    this.storageConnector = storageConnector;
    this.trainSplit = trainSplit;
    this.splits = splits == null ? Lists.newArrayList() : splits;
    this.seed = seed;
    this.featureStore = featureStore;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.label = label != null ? label.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.eventStartTime = eventStartTime != null ? FeatureGroupUtils.getDateFromDateString(eventStartTime) : null;
    this.eventEndTime = eventEndTime != null ? FeatureGroupUtils.getDateFromDateString(eventEndTime) : null;
    this.trainingDatasetType = trainingDatasetType != null ? trainingDatasetType :
        getTrainingDatasetType(storageConnector);
    setValTestSplit(validationSize, testSize);
    setTimeSeriesSplits(timeSplitSize, trainStart, trainEnd, validationStart, validationEnd, testStart, testEnd);
    if (extraFilter != null) {
      this.extraFilter = new FilterLogic(extraFilter);
    }
    if (extraFilterLogic != null) {
      this.extraFilter = extraFilterLogic;
    }
  }

  public void save(Query query) throws FeatureStoreException, IOException {
    save(query,null);
  }

  /**
   * Create the training dataset based on the content of the feature store query.
   *
   * @param query        the query to save as training dataset
   * @param writeOptions options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save(Query query, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    this.queryInt = query;
    TrainingDataset trainingDataset = trainingDatasetEngine.save(this, this.queryInt, writeOptions, label);
    this.setStorageConnector(trainingDataset.getStorageConnector());
    codeEngine.saveCode(this);
    computeStatistics();
  }

  /**
   * Read the content of the training dataset.
   *
   * @return Spark Dataset containing the training dataset data
   * @throws FeatureStoreException if the training dataset has splits and the split was not specified
   */
  @Override
  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read("");
  }

  /**
   * Read the content of the training dataset.
   *
   * @param readOptions options to pass to the Spark read operation
   * @return Spark Dataset containing the training dataset data
   * @throws FeatureStoreException if the training dataset has splits and the split was not specified
   */
  public Dataset<Row> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return read("", readOptions);
  }

  /**
   * Read all a single split from the training dataset.
   *
   * @param split the split name
   * @return Spark Dataset containing the training dataset data
   */
  public Dataset<Row> read(String split) throws FeatureStoreException, IOException {
    return read(split, null);
  }

  /**
   * Read a single split from the training dataset.
   *
   * @param split       the split name
   * @param readOptions options to pass to the Spark read operation
   * @return Spark Dataset containing the training dataset data
   * @throws FeatureStoreException if the training dataset has splits and the split was not specified
   */
  @Override
  public Dataset<Row> read(String split, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    if (this.splits != null && !this.splits.isEmpty() && Strings.isNullOrEmpty(split)) {
      throw new FeatureStoreException("The training dataset has splits, please specify the split you want to read");
    }
    return trainingDatasetEngine.read(this, split, readOptions);
  }

  /**
   * Show numRows from the training dataset (across all splits).
   *
   * @param numRows
   */
  @Override
  public void show(int numRows) throws FeatureStoreException, IOException {
    read("").show(numRows);
  }

  /**
   * Recompute the statistics for the entire training dataset and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public Statistics computeStatistics() throws FeatureStoreException, IOException {
    if (statisticsConfig.getEnabled()) {
      if (this.splits != null && !this.splits.isEmpty()) {
        return statisticsEngine.registerSplitStatistics(this);
      } else {
        return statisticsEngine.computeStatistics(this, read());
      }
    }
    return null;
  }

  /**
   * Update the statistics configuration of the training dataset.
   * Change the `enabled`, `histograms`, `correlations` or `columns` attributes and persist
   * the changes by calling this method.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public void updateStatisticsConfig() throws FeatureStoreException, IOException {
    trainingDatasetEngine.updateStatisticsConfig(this);
  }

  /**
   * Get the last statistics commit for the training dataset.
   *
   * @return statistics object of latest commit
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    return statisticsEngine.getLast(this);
  }

  /**
   * Get the statistics of a specific commit time for the training dataset.
   *
   * @param commitTime commit time in the format "YYYYMMDDhhmmss"
   * @return statistics object for the commit time
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  @Override
  public Statistics getStatistics(String commitTime) throws FeatureStoreException, IOException {
    return statisticsEngine.get(this, commitTime);
  }

  /**
   * Add name/value tag to the training dataset.
   *
   * @param name  name of the tag
   * @param value value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
    trainingDatasetEngine.addTag(this, name, value);
  }

  /**
   * Get all tags of the training dataset.
   *
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  @Override
  public Map<String, Object> getTags() throws FeatureStoreException, IOException {
    return trainingDatasetEngine.getTags(this);
  }

  /**
   * Get a single tag value of the training dataset.
   *
   * @param name name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  @Override
  public Object getTag(String name) throws FeatureStoreException, IOException {
    return trainingDatasetEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the training dataset.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    trainingDatasetEngine.deleteTag(this, name);
  }

  @JsonIgnore
  public String getQuery() throws FeatureStoreException, IOException {
    return getQuery(Storage.ONLINE, false);
  }

  @JsonIgnore
  public String getQuery(boolean withLabel) throws FeatureStoreException, IOException {
    return getQuery(Storage.ONLINE, withLabel);
  }

  @JsonIgnore
  public String getQuery(Storage storage) throws FeatureStoreException, IOException {
    return getQuery(storage, false);
  }

  @JsonIgnore
  @Override
  public String getQuery(Storage storage, boolean withLabel) throws FeatureStoreException, IOException {
    return trainingDatasetEngine.getQuery(this, storage, withLabel, false);
  }

  /**
   * Delete training dataset and all associated metadata.
   * Note that this operation drops only files which were materialized in
   * HopsFS. If you used a Storage Connector for a cloud storage such as S3,
   * the data will not be deleted, but you will not be able to track it anymore
   * from the Feature Store.
   * This operation drops all metadata associated with this version of the
   * training dataset and and the materialized data in HopsFS.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public void delete() throws FeatureStoreException, IOException {
    trainingDatasetEngine.delete(this);
  }
}
