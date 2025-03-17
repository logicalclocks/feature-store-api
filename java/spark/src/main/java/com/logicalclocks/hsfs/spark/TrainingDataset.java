/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDatasetType;
import com.logicalclocks.hsfs.constructor.Filter;
import com.logicalclocks.hsfs.constructor.FilterLogic;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.spark.engine.StatisticsEngine;
import com.logicalclocks.hsfs.spark.engine.TrainingDatasetEngine;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;

import com.logicalclocks.hsfs.TrainingDatasetBase;
import com.logicalclocks.hsfs.metadata.Statistics;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class TrainingDataset extends TrainingDatasetBase {

  private TrainingDatasetEngine trainingDatasetEngine = new TrainingDatasetEngine();
  private StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.TRAINING_DATASET);

  @Builder(builderMethodName = "buildTrainingDataset")
  public TrainingDataset(Integer version, String description, DataFormat dataFormat,
                         Boolean coalesce, StorageConnector storageConnector, String location, List<Split> splits,
                         String trainSplit, Long seed, FeatureStoreBase featureStore, StatisticsConfig statisticsConfig,
                         List<String> label, String eventStartTime, String eventEndTime,
                         TrainingDatasetType trainingDatasetType, Float validationSize, Float testSize,
                         String trainStart, String trainEnd, String validationStart,
                         String validationEnd, String testStart, String testEnd, Integer timeSplitSize,
                         FilterLogic extraFilterLogic, Filter extraFilter)
      throws FeatureStoreException, ParseException {
    this.version = version;
    this.description = description;
    this.dataFormat = dataFormat != null ? dataFormat : DataFormat.PARQUET;
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

    if (this.splits != null && !this.splits.isEmpty() && Strings.isNullOrEmpty(this.trainSplit)) {
      LOGGER.info("Training dataset splits were defined but no `trainSplit` (the name of the split that is going to"
          + " be used for training) was provided. Setting this property to `train`.");
      this.trainSplit = "train";
    }
  }

  /**
   * Read the content of the training dataset.
   *
   * @return Spark Dataset containing the training dataset data
   * @throws FeatureStoreException if the training dataset has splits and the split was not specified
   * @throws IOException IOException
   */
  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read("");
  }

  /**
   * Read the content of the training dataset.
   *
   * @param readOptions options to pass to the Spark read operation
   * @return Spark Dataset containing the training dataset data
   * @throws FeatureStoreException if the training dataset has splits and the split was not specified
   * @throws IOException IOException
   */
  public Dataset<Row> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return read("", readOptions);
  }

  /**
   * Read all a single split from the training dataset.
   *
   * @param split the split name
   * @return Spark Dataset containing the training dataset data
   * @throws FeatureStoreException if the training dataset has splits and the split was not specified
   * @throws IOException IOException
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
   * @throws IOException IOException
   */
  public Dataset<Row> read(String split, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    if (this.splits != null && !this.splits.isEmpty() && Strings.isNullOrEmpty(split)) {
      throw new FeatureStoreException("The training dataset has splits, please specify the split you want to read");
    }
    return trainingDatasetEngine.read(this, split, readOptions);
  }

  /**
   * Show numRows from the training dataset (across all splits).
   *
   * @param numRows number of rows to display
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void show(int numRows) throws FeatureStoreException, IOException {
    read("").show(numRows);
  }

  /**
   * Recompute the statistics for the entire training dataset and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public Statistics computeStatistics() throws FeatureStoreException, IOException {
    if (statisticsConfig.getEnabled()) {
      if (this.splits != null && !this.splits.isEmpty()) {
        return statisticsEngine.computeAndSaveSplitStatistics(this);
      } else {
        return statisticsEngine.computeStatistics(this, read());
      }
    }
    return null;
  }
}
