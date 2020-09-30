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
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.engine.TrainingDatasetEngine;
import com.logicalclocks.hsfs.metadata.Query;
import com.logicalclocks.hsfs.metadata.Statistics;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class TrainingDataset {
  @Getter @Setter
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private Integer version;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private DataFormat dataFormat;

  @Getter @Setter
  private TrainingDatasetType trainingDatasetType = TrainingDatasetType.HOPSFS_TRAINING_DATASET;

  @Getter @Setter
  private List<TrainingDatasetFeature> features;

  @Getter @Setter
  @JsonIgnore
  private FeatureStore featureStore;

  @Getter @Setter
  private Integer storageConnectorId;

  @Getter @Setter
  @JsonIgnore
  private StorageConnector storageConnector;

  @Getter @Setter
  private String location;

  @Getter @Setter
  private Long seed;

  @Getter @Setter
  private List<Split> splits;

  @Getter @Setter
  @JsonIgnore
  private Boolean statisticsEnabled = true;

  @Getter @Setter
  @JsonIgnore
  private Boolean histograms;

  @Getter @Setter
  @JsonIgnore
  private Boolean correlations;

  @Getter @Setter
  @JsonIgnore
  private List<String> statisticColumns;

  @Getter @Setter
  @JsonProperty("queryDTO")
  private Query queryInt;

  private TrainingDatasetEngine trainingDatasetEngine = new TrainingDatasetEngine();
  private StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.TRAINING_DATASET);

  @Builder
  public TrainingDataset(@NonNull String name, Integer version, String description, DataFormat dataFormat,
                         StorageConnector storageConnector, String location, List<Split> splits, Long seed,
                         FeatureStore featureStore, Boolean statisticsEnabled, Boolean histograms,
                         Boolean correlations, List<String> statisticColumns) {
    this.name = name;
    this.version = version;
    this.description = description;
    this.dataFormat = dataFormat != null ? dataFormat : DataFormat.TFRECORDS;
    this.location = location;
    this.storageConnector = storageConnector;

    if (storageConnector != null) {
      this.storageConnectorId = storageConnector.getId();
      if (storageConnector.getStorageConnectorType() == StorageConnectorType.S3) {
        // Default it's already HOPSFS_TRAINING_DATASET
        this.trainingDatasetType = TrainingDatasetType.EXTERNAL_TRAINING_DATASET;
      }
    }

    this.splits = splits;
    this.seed = seed;
    this.featureStore = featureStore;
    this.statisticsEnabled = statisticsEnabled != null ? statisticsEnabled : true;
    this.histograms = histograms;
    this.correlations = correlations;
    this.statisticColumns = statisticColumns;
  }

  /**
   * Create the training dataset based on the content of the feature store query.
   *
   * @param query the query to save as training dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save(Query query) throws FeatureStoreException, IOException {
    save(query, null);
  }

  /**
   * Create the training dataset based on teh content of the dataset.
   *
   * @param dataset the dataset to save as training dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save(Dataset<Row> dataset) throws FeatureStoreException, IOException {
    save(dataset, null);
  }

  /**
   * Create the training dataset based on the content of the feature store query.
   *
   * @param query the query to save as training dataset
   * @param writeOptions options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save(Query query, Map<String, String> writeOptions) throws FeatureStoreException, IOException {
    this.queryInt = query;
    save(query.read(), writeOptions);
  }

  /**
   * Create the training dataset based on teh content of the dataset.
   *
   * @param dataset the dataset to save as training dataset
   * @param writeOptions options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save(Dataset<Row> dataset, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    trainingDatasetEngine.save(this, dataset, writeOptions);
    if (statisticsEnabled) {
      statisticsEngine.computeStatistics(this, dataset);
    }
  }

  /**
   * Insert the content of the feature store query in the training dataset.
   *
   * @param query the query to write as training dataset
   * @param overwrite true to overwrite the current content of the training dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Query query, boolean overwrite) throws FeatureStoreException, IOException {
    insert(query, overwrite, null);
  }

  /**
   * Insert the content of the dataset in the training dataset.
   *
   * @param dataset the dataset to write as training dataset
   * @param overwrite true to overwrite the current content of the training dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Dataset<Row> dataset, boolean overwrite) throws FeatureStoreException, IOException {
    insert(dataset, overwrite, null);
  }

  /**
   * Insert the content of the feature store query in the training dataset.
   *
   * @param query the query to execute to generate the training dataset
   * @param overwrite true to overwrite the current content of the training dataset
   * @param writeOptions options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Query query, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    trainingDatasetEngine.insert(this, query.read(),
        writeOptions, overwrite ? SaveMode.Overwrite : SaveMode.Append);
    computeStatistics();
  }

  /**
   * Insert the content of the dataset in the training dataset.
   *
   * @param dataset the spark dataframe to write as training dataset
   * @param overwrite true to overwrite the current content of the training dataset
   * @param writeOptions options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Dataset<Row> dataset, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    trainingDatasetEngine.insert(this, dataset,
        writeOptions, overwrite ? SaveMode.Overwrite : SaveMode.Append);
    computeStatistics();
  }

  /**
   * Read the content (all splits if multiple available) of the training dataset.
   *
   * @return
   */
  public Dataset<Row> read() {
    return read("");
  }

  /**
   * Read the content (all splits if multiple available) of the training dataset.
   *
   * @param readOptions options to pass to the Spark read operation
   * @return
   */
  public Dataset<Row> read(Map<String, String> readOptions) {
    return trainingDatasetEngine.read(this, "", readOptions);
  }

  /**
   * Read all a single split from the training dataset.
   *
   * @param split the split name
   * @return
   */
  public Dataset<Row> read(String split) {
    return read(split, null);
  }


  /**
   * Read a single split from the training dataset.
   *
   * @param split the split name
   * @param readOptions options to pass to the Spark read operation
   * @return
   */
  public Dataset<Row> read(String split, Map<String, String> readOptions) {
    return trainingDatasetEngine.read(this, split, readOptions);
  }

  /**
   * Show numRows from the training dataset (across all splits).
   *
   * @param numRows
   */
  public void show(int numRows) {
    read("").show(numRows);
  }

  /**
   * Recompute the statistics for the entire training dataset and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Statistics computeStatistics() throws FeatureStoreException, IOException {
    if (statisticsEnabled) {
      return statisticsEngine.computeStatistics(this, read());
    }
    return null;
  }

  /**
   * Get the last statistics commit for the training dataset.
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
   * Get the statistics of a specific commit time for the training dataset.
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

  /**
   * Add a tag without value to the training dataset.
   *
   * @param name name of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name) throws FeatureStoreException, IOException {
    addTag(name, null);
  }

  /**
   * Add name/value tag to the training dataset.
   *
   * @param name name of the tag
   * @param value value of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name, String value) throws FeatureStoreException, IOException {
    trainingDatasetEngine.addTag(this, name, value);
  }

  /**
   * Get all tags of the training dataset.
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
   * Get a single tag value of the training dataset.
   *
   * @param name name of tha tag
   * @return string value of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, String> getTag(String name) throws FeatureStoreException, IOException {
    return trainingDatasetEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the training dataset.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    trainingDatasetEngine.deleteTag(this, name);
  }

  @JsonIgnore
  public String getQuery() throws FeatureStoreException, IOException  {
    return getQuery(Storage.ONLINE);
  }

  @JsonIgnore
  public String getQuery(Storage storage) throws FeatureStoreException, IOException {
    return trainingDatasetEngine.getQuery(this, storage);
  }
}
