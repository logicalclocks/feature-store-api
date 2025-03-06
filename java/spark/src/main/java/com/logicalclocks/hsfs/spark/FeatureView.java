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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.spark.constructor.Query;
import com.logicalclocks.hsfs.spark.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TrainingDatasetType;
import com.logicalclocks.hsfs.constructor.Filter;
import com.logicalclocks.hsfs.constructor.FilterLogic;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class FeatureView extends FeatureViewBase<FeatureView, FeatureStore, Query, Dataset<Row>> {

  private static final FeatureViewEngine featureViewEngine = new FeatureViewEngine();

  public static class FeatureViewBuilder {

    private String name;
    private Integer version;
    private String description;
    private FeatureStore featureStore;
    private Query query;
    private List<String> labels;

    public FeatureViewBuilder(FeatureStore featureStore) {
      this.featureStore = featureStore;
    }

    public FeatureViewBuilder name(String name) {
      this.name = name;
      return this;
    }

    public FeatureViewBuilder version(Integer version) {
      this.version = version;
      return this;
    }

    public FeatureViewBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * Query of a feature view. Note that `as_of` argument in the `Query` will be ignored because feature view does
     * not support time travel query.
     *
     * @param query Query object
     * @return builder
     */
    public FeatureViewBuilder query(Query query) {
      this.query = query;
      if (query.isTimeTravel()) {
        FeatureViewBase.LOGGER.info("`as_of` argument in the `Query` will be ignored because "
            + "feature view does not support time travel query.");
      }
      return this;
    }

    public FeatureViewBuilder labels(List<String> labels) {
      this.labels = labels;
      return this;
    }

    public FeatureView build() throws FeatureStoreException, IOException {
      FeatureView featureView = new FeatureView(name, version, query, description, featureStore, labels);
      featureViewEngine.save(featureView, FeatureView.class);
      return featureView;
    }
  }

  public FeatureView(@NonNull String name, Integer version, @NonNull Query query, String description,
                     @NonNull FeatureStore featureStore, List<String> labels) {
    this.name = name;
    this.version = version;
    this.query = query;
    this.description = description;
    this.featureStore = featureStore;
    this.labels = labels != null ? labels.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
  }

  /**
   * Delete the feature view and all associated metadata and training data. This can delete corrupted feature view
   * which cannot be retrieved due to a corrupted query for example.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // delete feature view
   *        fv.clean();
   * }
   * </pre>
   *
   * @param featureStore Feature store metadata object.
   * @param featureViewName Name of feature view.
   * @param featureViewVersion Version of feature view.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void clean(FeatureStore featureStore, String featureViewName, Integer featureViewVersion)
      throws FeatureStoreException, IOException {
    featureViewEngine.delete(featureStore, featureViewName, featureViewVersion);
  }

  /**
   * Update the description of the feature view.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // update with new description
   *        fv.setDescription("Updated description");
   *        // delete feature view
   *        fv.update(fv);
   * }
   * </pre>
   *
   * @param other Updated FeatureView metadata Object.
   * @return FeatureView Metadata Object.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public FeatureView update(FeatureView other) throws FeatureStoreException, IOException {
    return featureViewEngine.update(other);
  }

  /**
   * Get a query string of the batch query.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // get batch query
   *        fv.getBatchQuery();
   * }
   * </pre>
   *
   * @return String query string of the batch query
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  @JsonIgnore
  public String getBatchQuery() throws FeatureStoreException, IOException, ParseException {
    return getBatchQuery(null, null);
  }

  /**
   * Get a query string of the batch query.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // get batch query that will fetch data from jan 1, 2023 to Jan 31, 2023
   *        fv.getBatchQuery("20230101", "20130131");
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return String query string of the batch query
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
  @JsonIgnore
  public String getBatchQuery(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    return featureViewEngine.getBatchQueryString(
        this,
        startTime != null ? FeatureGroupUtils.getDateFromDateString(startTime) : null,
        endTime != null ? FeatureGroupUtils.getDateFromDateString(endTime) : null,
        extraFilterVersion);
  }

  /**
   * Get all data from the feature view as a batch from the offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // get batch data
   *        fv.getBatchData();
   * }
   * </pre>
   *
   * @return {@code Dataset<Row>} Spark dataframe of batch data.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
  @JsonIgnore
  public Dataset<Row> getBatchData() throws FeatureStoreException, IOException, ParseException {
    return getBatchData(null, null, null);
  }

  /**
   * Get a batch of data from an event time interval from the offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // get batch query that will fetch data from jan 1, 2023 to Jan 31, 2023
   *        fv.getBatchData("20230101", "20130131");
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return {@code Dataset<Row>} Spark dataframe of batch data.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
  @JsonIgnore
  public Dataset<Row> getBatchData(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    return getBatchData(startTime, endTime, Maps.newHashMap());
  }

  /**
   * Get a batch of data from an event time interval from the offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        Map<String, String> readOptions =  new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // get batch query that will fetch data from jan 1, 2023 to Jan 31, 2023
   *        fv.getBatchData("20230101", "20130131");
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param readOptions Additional read options as key/value pairs.
   * @return {@code Dataset<Row>} Spark dataframe of batch data.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
  @JsonIgnore
  public Dataset<Row> getBatchData(String startTime, String endTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return featureViewEngine.getBatchData(
        this,
        startTime != null ? FeatureGroupUtils.getDateFromDateString(startTime) : null,
        endTime != null ? FeatureGroupUtils.getDateFromDateString(endTime) : null,
        readOptions,
        extraFilterVersion
    );
  }

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data can be retrieved by calling `feature_view.getTrainingData()`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset
   *        String startTime = "20220101000000";
   *        String endTime = "20220606235959";
   *        String description = "demo training dataset":
   *        fv.createTrainingData(startTime, endTime, description, DataFormat.CSV);
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param dataFormat  The data format used to save the training dataset.
   * @return Integer Training dataset version.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
  public Integer createTrainingData(
      String startTime, String endTime, String description, DataFormat dataFormat
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .dataFormat(dataFormat)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, null).getVersion();
  }

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data can be retrieved by calling `featureView.getTrainingData()`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset
   *        String startTime = "20220101000000";
   *        String endTime = "20220606235959";
   *        String description = "demo training dataset":
   *        String location = "";
   *        StatisticsConfig statisticsConfig = new StatisticsConfig(true, true, true, true)
   *        fv.createTrainingData(startTime, endTime, description, DataFormat.CSV, true, location, statisticsConfig);
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param dataFormat  The data format used to save the training dataset.
   * @param coalesce If true the training dataset data will be coalesced into a single partition before writing.
   *                 The resulting training dataset will be a single file per split.
   * @param storageConnector Storage connector defining the sink location for the  training dataset. If  `null` is
   *                         provided  and materializes training dataset on HopsFS.
   * @param location Path to complement the sink storage connector with, e.g if the storage connector points to an
   *                 S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training
   *                 dataset.  If empty string is provided `""`, saving the training dataset at the root defined by the
   *                 storage connector.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param writeOptions Additional write options as key-value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return Integer Training dataset version.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
  public Integer createTrainingData(String startTime, String endTime, String description, DataFormat dataFormat,
                                    Boolean coalesce, StorageConnector storageConnector,
                                    String location, Long seed, StatisticsConfig statisticsConfig,
                                    Map<String, String> writeOptions, FilterLogic extraFilterLogic, Filter extraFilter)
      throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .dataFormat(dataFormat)
            .coalesce(coalesce)
            .storageConnector(storageConnector)
            .location(location)
            .seed(seed)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, writeOptions).getVersion();
  }

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data is split into train and test set at random or according to time ranges. The training data can be retrieved by
   * calling `featureView.getTrainTestSplit` method.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String testStart = "20220701000000";
   *        String testEnd = "20220830235959";
   *        String description = "demo training dataset":
   *        fv.createTrainTestSplit(null, trainStart, trainEnd, testStart, testEnd, description, DataFormat.CSV);
   *
   *        // or based on random split
   *        fv.createTrainTestSplit(30, null, null, null, null, description, DataFormat.CSV);
   * }
   * </pre>
   *
   * @param testSize Size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param dataFormat  The data format used to save the training dataset.
   * @return Integer Training dataset version
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public Integer createTrainTestSplit(
      Float testSize, String trainStart, String trainEnd, String testStart, String testEnd,
      String description, DataFormat dataFormat
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainTestSplit(testSize, trainEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .dataFormat(dataFormat)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(2)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, null).getVersion();
  }

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data is split into train and test set at random or according to time ranges. The training data can be retrieved by
   * calling `featureView.getTrainTestSplit` method.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String testStart = "20220701000000";
   *        String testEnd = "20220830235959";
   *        String description = "demo training dataset":
   *        StatisticsConfig statisticsConfig = new StatisticsConfig(true, true, true, true)
   *        Map<String, String> writeOptions = new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // define extra filters
   *        Filter leftFtFilter = new Filter();
   *        leftFtFilter.setFeature(new Feature("left_ft_name"));
   *        leftFtFilter.setValue("400");
   *        leftFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        Filter rightFtFilter = new Filter();
   *        rightFtFilter.setFeature(new Feature("right_ft_name"));
   *        rightFtFilter.setValue("50");
   *        rightFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        FilterLogic extraFilterLogic = new FilterLogic(SqlFilterLogic.AND, leftFtFilter, rightFtFilter);
   *        Filter extraFilter = new Filter();
   *        extraFilter.setFeature(new Feature("ft_name"));
   *        extraFilter.setValue("100");
   *        extraFilter.setCondition(SqlFilterCondition.GREATER_THAN);
   *
   *        // create training data
   *        fv.createTrainTestSplit(null, null, trainStart, trainEnd, testStart,
   *        testEnd,  description, DataFormat.CSV, coalesce, storageConnector, location, seed, statisticsConfig,
   *        writeOptions, extraFilterLogic, extraFilter);
   *
   *        // or based on random split
   *        fv.createTrainTestSplit(20, 10, null, null,  null, null, description, DataFormat.CSV, coalesce,
   *        storageConnector, location, seed, statisticsConfig, writeOptions, extraFilterLogic, extraFilter);

   * }
   * </pre>
   *
   * @param testSize Size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param dataFormat  The data format used to save the training dataset.
   * @param coalesce If true the training dataset data will be coalesced into a single partition before writing.
   *                 The resulting training dataset will be a single file per split.
   * @param storageConnector Storage connector defining the sink location for the  training dataset. If  `null` is
   *                         provided  and materializes training dataset on HopsFS.
   * @param location Path to complement the sink storage connector with, e.g if the storage connector points to an
   *                 S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training
   *                 dataset.  If empty string is provided `""`, saving the training dataset at the root defined by the
   *                 storage connector.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param writeOptions Additional write options as key-value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return Integer Training dataset version.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public Integer createTrainTestSplit(
      Float testSize, String trainStart, String trainEnd, String testStart, String testEnd,
      String description, DataFormat dataFormat, Boolean coalesce,
      StorageConnector storageConnector, String location,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> writeOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainTestSplit(testSize, trainEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .dataFormat(dataFormat)
            .coalesce(coalesce)
            .storageConnector(storageConnector)
            .location(location)
            .trainSplit(Split.TRAIN)
            .seed(seed)
            .timeSplitSize(2)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, writeOptions).getVersion();
  }

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data is split into train, validation, and test set at random or according to time range. The training data can be
   * retrieved by calling `featureView.getTrainValidationTestSplit`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String validationStart = "20220701000000";
   *        String validationEnd = "20220830235959";
   *        String testStart = "20220901000000";
   *        String testEnd = "20220931235959";
   *        String description = "demo training dataset":
   *        fv.createTrainTestSplit(null, null, trainStart, trainEnd, validationStart, validationEnd, testStart,
   *        testEnd, description, DataFormat.CSV);
   *
   *        // or based on random split
   *        fv.createTrainTestSplit(20, 10, null, null, null, null, null, null, description, DataFormat.CSV);
   * }
   * </pre>
   *
   * @param validationSize Size of validation set.
   * @param testSize Size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                        `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                      `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param dataFormat  The data format used to save the training dataset.
   * @return Integer Training dataset version.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public Integer createTrainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description, DataFormat dataFormat
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainValidationTestSplit(validationSize, testSize, trainEnd, validationStart, validationEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .validationSize(validationSize)
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .validationStart(validationStart)
            .validationEnd(validationEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .dataFormat(dataFormat)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(3)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, null).getVersion();
  }

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data is split into train, validation, and test set at random or according to time range. The training data can be
   * retrieved by calling `feature_view.getTrainValidationTestSplit`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String validationStart = "20220701000000";
   *        String validationEnd = "20220830235959";
   *        String testStart = "20220901000000";
   *        String testEnd = "20220931235959";
   *        String description = "demo training dataset":
   *        StorageConnector.S3Connector storageConnector = fs.getS3Connector("s3Connector");
   *        String location = "";
   *        Long seed = 1234L;
   *        Boolean coalesce = true;
   *        StatisticsConfig statisticsConfig = new StatisticsConfig(true, true, true, true)
   *        Map<String, String> writeOptions = new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // define extra filters
   *        Filter leftFtFilter = new Filter();
   *        leftFtFilter.setFeature(new Feature("left_ft_name"));
   *        leftFtFilter.setValue("400");
   *        leftFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        Filter rightFtFilter = new Filter();
   *        rightFtFilter.setFeature(new Feature("right_ft_name"));
   *        rightFtFilter.setValue("50");
   *        rightFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        FilterLogic extraFilterLogic = new FilterLogic(SqlFilterLogic.AND, leftFtFilter, rightFtFilter);
   *        Filter extraFilter = new Filter();
   *        extraFilter.setFeature(new Feature("ft_name"));
   *        extraFilter.setValue("100");
   *        extraFilter.setCondition(SqlFilterCondition.GREATER_THAN);
   *        // create training data
   *        fv.createTrainTestSplit(null, null, trainStart, trainEnd, validationStart, validationEnd, testStart,
   *        testEnd,  description, DataFormat.CSV, coalesce, storageConnector, location, seed, statisticsConfig,
   *        writeOptions, extraFilterLogic, extraFilter);
   *
   *        // or based on random split
   *        fv.createTrainTestSplit(20, 10, null, null, null, null, null, null, description, DataFormat.CSV, coalesce,
   *        storageConnector, location, seed, statisticsConfig, writeOptions, extraFilterLogic, extraFilter);
   * }
   * </pre>
   *
   * @param validationSize Size of validation set.
   * @param testSize Size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                        `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                      `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param dataFormat  The data format used to save the training dataset.
   * @param coalesce If true the training dataset data will be coalesced into a single partition before writing.
   *                 The resulting training dataset will be a single file per split.
   * @param storageConnector Storage connector defining the sink location for the  training dataset. If  `null` is
   *                         provided  and materializes training dataset on HopsFS.
   * @param location Path to complement the sink storage connector with, e.g if the storage connector points to an
   *                 S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training
   *                 dataset.  If empty string is provided `""`, saving the training dataset at the root defined by the
   *                 storage connector.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param writeOptions Additional write options as key-value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return Integer Training dataset version.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public Integer createTrainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description, DataFormat dataFormat,
      Boolean coalesce, StorageConnector storageConnector, String location,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> writeOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainValidationTestSplit(validationSize, testSize, trainEnd, validationStart, validationEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .validationSize(validationSize)
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .validationStart(validationStart)
            .validationEnd(validationEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .dataFormat(dataFormat)
            .coalesce(coalesce)
            .storageConnector(storageConnector)
            .location(location)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(3)
            .seed(seed)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, writeOptions).getVersion();
  }

  private List<Dataset<Row>> getDataset(TrainingDatasetBundle trainingDatasetBundle, List<String> splits) {
    List<Dataset<Row>> features = Lists.newArrayList();
    List<Dataset<Row>> labels = Lists.newArrayList();
    for (String split: splits) {
      List<Dataset<Row>> featureLabel = trainingDatasetBundle.getDataset(split, true);
      features.add(featureLabel.get(0));
      labels.add(featureLabel.get(1));
    }
    features.addAll(labels);
    return features;
  }

  /**
   * Recreate a training dataset.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // define write options
   *        Map<String, String> writeOptions = new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        //recreate training data
   *        fv.recreateTrainingDataset(1, writeOptions);
   * }
   * </pre>
   *
   * @param version Training dataset version.
   * @param writeOptions Additional read options as key-value pairs.
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void recreateTrainingDataset(Integer version, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureViewEngine.recreateTrainingDataset(this, version, writeOptions);
  }

  /**
   * Get training data created by `featureView.createTrainingData` or `featureView.trainingData`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // get training data
   *        fv.getTrainingData(1);
   * }
   * </pre>
   *
   * @param version Training dataset version.
   * @return {@code List<Dataset<Row>>} List of dataframe of features and labels.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainingData(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainingData(version, null);
  }

  /**
   * Get training data created by `featureView.createTrainingData` or `featureView.trainingData`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // define write options
   *        Map<String, String> readOptions = new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // get training data
   *        fv.getTrainingData(1, readOptions);
   * }
   * </pre>
   *
   * @param version Training dataset version.
   * @param readOptions Additional read options as key/value pairs.
   * @return {@code List<Dataset<Row>>} List of dataframe of features and labels.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainingData(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(), readOptions)
        .getDataset(true);
  }

  /**
   * Get training data created by `featureView.createTrainTestSplit` or `featureView.trainTestSplit`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // get train test split dataframe of features and labels
   *        fv.getTrainTestSplit(1);
   * }
   * </pre>
   *
   * @param version Training dataset version.
   * @return {@code List<Dataset<Row>>} List of dataframe of features and labels.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainTestSplit(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainTestSplit(version, null);
  }

  /**
   * Get training data created by `featureView.createTrainTestSplit` or `featureView.trainTestSplit`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // define additional readOptions
   *        Map<String, String> readOptions =  new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // get train test split dataframe of features and labels
   *        fv.getTrainTestSplit(1, readOptions);
   * }
   * </pre>
   *
   * @param version Training dataset version.
   * @param readOptions Additional read options as key/value pairs.
   * @return {@code List<Dataset<Row>>} List of dataframe of features and labels.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainTestSplit(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return getDataset(
        featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(Split.TRAIN, Split.TEST), readOptions),
        Lists.newArrayList(Split.TRAIN, Split.TEST));
  }

  /**
   * Get training data created by `featureView.createTrainValidationTestSplit` or featureView.trainValidationTestSplit`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // get train, validation, test split dataframe of features and labels
   *        fv.getTrainValidationTestSplit(1);
   * }
   * </pre>
   *
   * @param version Training dataset version.
   * @return {@code List<Dataset<Row>>} List of dataframe of features and labels.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainValidationTestSplit(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainValidationTestSplit(version, null);
  }

  /**
   * Get training data created by `featureView.createTrainValidationTestSplit` or featureView.trainValidationTestSplit`.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // define additional readOptions
   *        Map<String, String> readOptions =  new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // get train, validation, test split dataframe of features and labels
   *        fv.getTrainValidationTestSplit(1, readOptions);
   * }
   * </pre>
   *
   * @param version Training dataset version.
   * @param readOptions Additional read options as key/value pairs.
   * @return {@code List<Dataset<Row>>} List of dataframe of features and labels.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainValidationTestSplit(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return getDataset(
        featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST), readOptions),
        Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST));
  }

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data can be
   * recreated by calling `featureView.getTrainingData` with the metadata created.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String startTime = "20220101000000";
   *        String endTime = "20220630235959";
   *        String description = "demo training dataset":
   *        fv.createTrainTestSplit(startTime, endTime, description);
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @return {@code List<Dataset<Row>>} List of dataframe of features and labels.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public List<Dataset<Row>> trainingData(
      String startTime, String endTime, String description
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, null).getDataset(true);
  }

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data can be
   * recreated by calling `featureView.getTrainingData` with the metadata created.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String startTime = "20220101000000";
   *        String endTime = "20220630235959";
   *        String description = "demo training dataset":
   *        Long seed = 1234L;
   *        StatisticsConfig statisticsConfig = new StatisticsConfig(true, true, true, true)
   *        Map<String, String> readOptions =  new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // define extra filters
   *        Filter leftFtFilter = new Filter();
   *        leftFtFilter.setFeature(new Feature("left_ft_name"));
   *        leftFtFilter.setValue("400");
   *        leftFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        Filter rightFtFilter = new Filter();
   *        rightFtFilter.setFeature(new Feature("right_ft_name"));
   *        rightFtFilter.setValue("50");
   *        rightFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        FilterLogic extraFilterLogic = new FilterLogic(SqlFilterLogic.AND, leftFtFilter, rightFtFilter);
   *        Filter extraFilter = new Filter();
   *        extraFilter.setFeature(new Feature("ft_name"));
   *        extraFilter.setValue("100");
   *        extraFilter.setCondition(SqlFilterCondition.GREATER_THAN);
   *        // create training data
   *        fv.trainValidationTestSplit(startTime, endTime,  description, seed, statisticsConfig, readOptions,
   *        extraFilterLogic, extraFilter);
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability.
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param readOptions Additional read options as key/value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return {@code List<Dataset<Row>>} List of dataframe of features and labels.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public List<Dataset<Row>> trainingData(
      String startTime, String endTime, String description,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> readOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .seed(seed)
            .statisticsConfig(statisticsConfig)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, readOptions).getDataset(true);
  }

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data is split
   * into train and test set at random or according to time ranges. The training data can be recreated by calling
   * `featureView.getTrainTestSplit` with the metadata created.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String testStart = "20220701000000";
   *        String testEnd = "20220830235959";
   *        String description = "demo training dataset":
   *        // create training data
   *        fv.trainValidationTestSplit(null, trainStart, trainEnd, testStart, trainEnd, description);
   *        // or random split
   *        fv.trainValidationTestSplit(30, null, null, null, null, description);
   * }
   * </pre>
   *
   * @param testSize Size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @return {@code List<Dataset<Row>>} List of Spark Dataframes containing training dataset splits.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public List<Dataset<Row>> trainTestSplit(
      Float testSize, String trainStart, String trainEnd, String testStart, String testEnd,
      String description
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainTestSplit(testSize, trainEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(2)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .build();
    return getDataset(
        featureViewEngine.getTrainingDataset(this, trainingDataset, null),
        Lists.newArrayList(Split.TRAIN, Split.TEST));
  }

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data is split into
   * train and test set at random or according to time ranges. The training data can be recreated by calling
   * `feature_view.getTrainTestSplit` with the metadata created.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String testStart = "20220701000000";
   *        String testEnd = "20220830235959";
   *        String description = "demo training dataset":
   *        Long seed = 1234L;
   *        StatisticsConfig statisticsConfig = new StatisticsConfig(true, true, true, true)
   *        Map<String, String> readOptions =  new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // define extra filters
   *        Filter leftFtFilter = new Filter();
   *        leftFtFilter.setFeature(new Feature("left_ft_name"));
   *        leftFtFilter.setValue("400");
   *        leftFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        Filter rightFtFilter = new Filter();
   *        rightFtFilter.setFeature(new Feature("right_ft_name"));
   *        rightFtFilter.setValue("50");
   *        rightFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        FilterLogic extraFilterLogic = new FilterLogic(SqlFilterLogic.AND, leftFtFilter, rightFtFilter);
   *        Filter extraFilter = new Filter();
   *        extraFilter.setFeature(new Feature("ft_name"));
   *        extraFilter.setValue("100");
   *        extraFilter.setCondition(SqlFilterCondition.GREATER_THAN);
   *        // create training data
   *        fv.trainTestSplit(null, strainStart, trainEnd, testStart, trainEnd, description, seed, statisticsConfig,
   *        readOptions, extraFilterLogic, extraFilter);
   *
   *        // or random split
   *        fv.trainTestSplit(30, null, null, null, null, description, seed, statisticsConfig, readOptions,
   *        extraFilterLogic, extraFilter);
   * }
   * </pre>
   *
   * @param testSize Size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability.
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param readOptions Additional read options as key/value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return {@code List<Dataset<Row>>} List of Spark Dataframes containing training dataset splits.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public List<Dataset<Row>> trainTestSplit(
      Float testSize, String trainStart, String trainEnd, String testStart, String testEnd,
      String description, Long seed, StatisticsConfig statisticsConfig, Map<String, String> readOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainTestSplit(testSize, trainEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(2)
            .seed(seed)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return getDataset(
        featureViewEngine.getTrainingDataset(this, trainingDataset, readOptions),
        Lists.newArrayList(Split.TRAIN, Split.TEST));
  }

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data is split into
   * train, validation, and test set at random or according to time ranges. The training data can be recreated by
   * calling `feature_view.getTrainValidationTestSplit` with the metadata created.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String validationStart = "20220701000000";
   *        String validationEnd = "20220830235959";
   *        String testStart = "20220901000000";
   *        String testEnd = "20220931235959";
   *        String description = "demo training dataset":
   *        fv.trainValidationTestSplit(null, null, trainStart, trainEnd, validationStart, validationEnd, testStart,
   *        testEnd,  description);
   *
   *        // or based on random split
   *        fv.trainValidationTestSplit(20, 10, null, null, null, null, null, null, description);
   * }
   * </pre>
   *
   * @param validationSize Size of validation set.
   * @param testSize Size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                        `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                      `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @return {@code List<Dataset<Row>>} List of Spark Dataframes containing training dataset splits.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public List<Dataset<Row>> trainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainValidationTestSplit(validationSize, testSize, trainEnd, validationStart, validationEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .validationSize(validationSize)
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .validationStart(validationStart)
            .validationEnd(validationEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(3)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .build();
    return getDataset(
        featureViewEngine.getTrainingDataset(this, trainingDataset, null),
        Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST));
  }

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data is split into
   * train, validation, and test set at random or according to time ranges. The training data can be recreated by
   * calling `featureView.getTrainValidationTestSplit` with the metadata created.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String validationStart = "20220701000000";
   *        String validationEnd = "20220830235959";
   *        String testStart = "20220901000000";
   *        String testEnd = "20220931235959";
   *        String description = "demo training dataset":
   *        Long seed = 1234L;
   *        StatisticsConfig statisticsConfig = new StatisticsConfig(true, true, true, true)
   *        Map<String, String> readOptions =  new HashMap<String, String>() {{
   *                           put("header", "true");
   *                           put("delimiter", ",")}
   *                           };
   *        // define extra filters
   *        Filter leftFtFilter = new Filter();
   *        leftFtFilter.setFeature(new Feature("left_ft_name"));
   *        leftFtFilter.setValue("400");
   *        leftFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        Filter rightFtFilter = new Filter();
   *        rightFtFilter.setFeature(new Feature("right_ft_name"));
   *        rightFtFilter.setValue("50");
   *        rightFtFilter.setCondition(SqlFilterCondition.EQUALS);
   *        FilterLogic extraFilterLogic = new FilterLogic(SqlFilterLogic.AND, leftFtFilter, rightFtFilter);
   *        Filter extraFilter = new Filter();
   *        extraFilter.setFeature(new Feature("ft_name"));
   *        extraFilter.setValue("100");
   *        extraFilter.setCondition(SqlFilterCondition.GREATER_THAN);
   *        // create training data
   *        fv.trainValidationTestSplit(null, null, trainStart, trainEnd, validationStart, validationEnd, testStart,
   *        testEnd,  description, seed, statisticsConfig,
   *        readOptions, extraFilterLogic, extraFilter);
   *
   *        // or based on random split
   *        fv.trainValidationTestSplit(20, 10, null, null, null, null, null, null, description, statisticsConfig,
   *        seed, readOptions, extraFilterLogic, extraFilter);
   * }
   * </pre>
   *
   * @param validationSize Size of validation set.
   * @param testSize Size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                        `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                      `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability.
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param readOptions Additional read options as key/value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return {@code List<Dataset<Row>>} List of Spark Dataframes containing training dataset splits.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
  public List<Dataset<Row>> trainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> readOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainValidationTestSplit(validationSize, testSize, trainEnd, validationStart, validationEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .validationSize(validationSize)
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .validationStart(validationStart)
            .validationEnd(validationEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(3)
            .seed(seed)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return getDataset(
        featureViewEngine.getTrainingDataset(this, trainingDataset, readOptions),
        Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST));
  }

  /**
   * Delete a training dataset (data only).
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // Delete a training dataset version 1
   *        fv.purgeAllTrainingData(1);
   * }
   * </pre>
   *
   * @param version Version of the training dataset to be removed.
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void purgeTrainingData(Integer version) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingDatasetOnly(this, version);
  }

  /**
   * Delete all training datasets in this feature view (data only).
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // Delete a training dataset.
   *        fv.purgeAllTrainingData(1);
   * }
   * </pre>
   *
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void purgeAllTrainingData() throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingDatasetOnly(this);
  }

  /**
   * Delete a training dataset. This will delete both metadata and training data.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // Delete a training dataset version 1.
   *        fv.deleteTrainingDataset(1);
   * }
   * </pre>
   *
   * @param version Version of the training dataset to be removed.
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void deleteTrainingDataset(Integer version) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingData(this, version);
  }

  /**
   * Delete all training datasets. This will delete both metadata and training data.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature view handle
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   *        // Delete all training datasets in this feature view.
   *        fv.deleteAllTrainingDatasets();
   * }
   * </pre>
   *
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void deleteAllTrainingDatasets() throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingData(this);
  }
}
