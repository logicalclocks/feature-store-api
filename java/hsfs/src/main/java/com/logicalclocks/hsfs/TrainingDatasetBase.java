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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.logicalclocks.hsfs.constructor.Filter;
import com.logicalclocks.hsfs.constructor.FilterLogic;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.metadata.Statistics;

import com.logicalclocks.hsfs.metadata.StatisticsApi;
import com.logicalclocks.hsfs.metadata.TagsApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class TrainingDatasetBase {
  @Getter
  @Setter
  protected Integer id;

  @Getter
  @Setter
  protected String name;

  @Getter
  @Setter
  protected Integer version;

  @Getter
  @Setter
  protected String description;

  @Getter
  @Setter
  protected Boolean coalesce;

  @Getter
  @Setter
  protected TrainingDatasetType trainingDatasetType = TrainingDatasetType.HOPSFS_TRAINING_DATASET;

  @Getter
  @Setter
  protected List<TrainingDatasetFeature> features;

  @Getter
  @Setter
  @JsonIgnore
  protected FeatureStoreBase featureStore;

  @Getter
  @Setter
  protected String location;

  @Getter
  @Setter
  protected Long seed;

  @Getter
  @Setter
  protected List<Split> splits;

  @Getter
  @Setter
  protected String trainSplit;

  @JsonIgnore
  protected List<String> label;

  @Getter
  @Setter
  protected Date eventStartTime;

  @Getter
  @Setter
  protected Date eventEndTime;

  @Getter
  @Setter
  protected FilterLogic extraFilter;

  @Getter
  @Setter
  protected DataFormat dataFormat;

  @Getter
  @Setter
  protected StorageConnector storageConnector;

  @Getter
  @Setter
  protected StatisticsConfig statisticsConfig = new StatisticsConfig();

  @Getter
  @Setter
  protected String type = "trainingDatasetDTO";

  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.TRAINING_DATASET);
  private StatisticsApi statisticsApi = new StatisticsApi(EntityEndpointType.TRAINING_DATASET);
  protected static final Logger LOGGER = LoggerFactory.getLogger(TrainingDatasetBase.class);

  @Builder
  public TrainingDatasetBase(Integer version, String description, DataFormat dataFormat,
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

  public void setTimeSeriesSplits(Integer timeSplitSize, String trainStart, String trainEnd, String valStart,
                                  String valEnd, String testStart, String testEnd) throws FeatureStoreException,
      ParseException {
    List<Split> splits = Lists.newArrayList();
    appendTimeSeriesSplit(splits, Split.TRAIN,
        trainStart, trainEnd != null ? trainEnd : valStart != null ? valStart : testStart);
    if (timeSplitSize != null && timeSplitSize == 3) {
      appendTimeSeriesSplit(splits, Split.VALIDATION,
          valStart != null ? valStart : trainEnd,
          valEnd != null ? valEnd : testStart);
    }
    appendTimeSeriesSplit(splits, Split.TEST,
        testStart != null ? testStart : valEnd != null ? valEnd : trainEnd,
        testEnd);
    if (!splits.isEmpty()) {
      this.splits = splits;
    }
  }

  private void appendTimeSeriesSplit(List<Split> splits, String splitName, String startTime, String endTime)
      throws FeatureStoreException, ParseException {
    if (startTime != null || endTime != null) {
      splits.add(
          new Split(splitName,
              FeatureGroupUtils.getDateFromDateString(startTime),
              FeatureGroupUtils.getDateFromDateString(endTime)));
    }
  }

  public void setValTestSplit(Float valSize, Float testSize) {
    if (valSize != null && testSize != null) {
      this.splits = Lists.newArrayList();
      this.splits.add(new Split(Split.TRAIN, 1 - valSize - testSize));
      this.splits.add(new Split(Split.VALIDATION, valSize));
      this.splits.add(new Split(Split.TEST, testSize));
    } else if (testSize != null) {
      this.splits = Lists.newArrayList();
      this.splits.add(new Split(Split.TRAIN, 1 - testSize));
      this.splits.add(new Split(Split.TEST, testSize));
    }
  }

  @JsonIgnore
  public List<String> getLabel() {
    return features.stream().filter(TrainingDatasetFeature::getLabel).map(TrainingDatasetFeature::getName).collect(
        Collectors.toList());
  }

  @JsonIgnore
  public void setLabel(List<String> label) {
    this.label = label.stream().map(String::toLowerCase).collect(Collectors.toList());
  }

  public TrainingDatasetType getTrainingDatasetType(StorageConnector storageConnector) {
    if (storageConnector == null) {
      return TrainingDatasetType.HOPSFS_TRAINING_DATASET;
    } else if (storageConnector.getStorageConnectorType() == StorageConnectorType.HOPSFS) {
      return TrainingDatasetType.HOPSFS_TRAINING_DATASET;
    } else {
      return TrainingDatasetType.EXTERNAL_TRAINING_DATASET;
    }
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
    return statisticsApi.getLast(this);
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
    return statisticsApi.get(this, commitTime);
  }

  /**
   * Add name/value tag to the training dataset.
   *
   * @param name  name of the tag
   * @param value value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
    tagsApi.add(this, name, value);
  }

  /**
   * Get all tags of the training dataset.
   *
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, Object> getTags() throws FeatureStoreException, IOException {
    return tagsApi.get(this);
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
  public Object getTag(String name) throws FeatureStoreException, IOException {
    return tagsApi.get(this, name);
  }

  /**
   * Delete a tag of the training dataset.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    tagsApi.deleteTag(this, name);
  }

  /**
   * Delete training dataset and all associated metadata.
   * Note that this operation drops only files which were materialized in
   * HopsFS. If you used a Storage Connector for a cloud storage such as S3,
   * the data will not be deleted, but you will not be able to track it anymore
   * from the Feature Store.
   * This operation drops all metadata associated with this version of the
   * training dataset and the materialized data in HopsFS.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void delete() throws FeatureStoreException, IOException {
    trainingDatasetApi.delete(this);
  }
}
