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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.spark.constructor.Query;
import com.logicalclocks.hsfs.spark.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.spark.engine.StatisticsEngine;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.Feature;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.JobConfiguration;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.engine.CodeEngine;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.Statistics;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup extends FeatureGroupBase<Dataset<Row>> {

  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  protected StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);

  @Builder
  public FeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version,
                      String description, List<String> primaryKeys, List<String> partitionKeys,
                      String hudiPrecombineKey, boolean onlineEnabled, TimeTravelFormat timeTravelFormat,
                      List<Feature> features, StatisticsConfig statisticsConfig, String onlineTopicName,
                      String topicName, String notificationTopicName, String eventTime) {
    this();
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
        ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
        ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = timeTravelFormat == TimeTravelFormat.HUDI && hudiPrecombineKey != null
        ? hudiPrecombineKey.toLowerCase() : null;
    this.onlineEnabled = onlineEnabled;
    this.timeTravelFormat = timeTravelFormat != null ? timeTravelFormat : TimeTravelFormat.HUDI;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.onlineTopicName = onlineTopicName;
    this.topicName = topicName;
    this.notificationTopicName = notificationTopicName;
    this.eventTime = eventTime;
  }

  public FeatureGroup() {
    this.type = "cachedFeaturegroupDTO";
  }

  public FeatureGroup(FeatureStore featureStore, Integer id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  // used for updates
  public FeatureGroup(Integer id, String description, List<Feature> features) {
    this();
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public FeatureGroup(FeatureStore featureStore, int id) {
    this();
    this.featureStore = featureStore;
    this.id = id;
  }

  /**
   * Reads the feature group from the offline storage as Spark DataFrame.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // read feature group
   *        fg.read()
   * }
   * </pre>
   *
   * @return Spark DataFrame containing the feature data.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no commit information was
   *                               found for this feature group;
   * @throws IOException Generic IO exception.
   */
  @Override
  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  /**
   * Reads the feature group from the offline or online storage as Spark DataFrame.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // read feature group data from online storage
   *        fg.read(true)
   *        // read feature group data from offline storage
   *        fg.read(false)
   * }
   * </pre>
   *
   * @param online Set `online` to `true` to read from the online storage.
   * @return Spark DataFrame containing the feature data.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no commit information was found
   *                               for this feature group;
   * @throws IOException Generic IO exception.
   */
  @Override
  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return selectAll().read(online);
  }

  /**
   * Reads the feature group from the offline storage as Spark DataFrame.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // define additional read options (this example applies to HUDI enabled FGs)
   *        Map<String, String> readOptions = new HashMap<String, String>() {{
   *                                                  put("hoodie.datasource.read.end.instanttime", "20230401211015")
   *                                                }};
   *        // read feature group data
   *        fg.read(readOptions)
   * }
   * </pre>
   *
   * @param readOptions Additional read options as key/value pairs.
   * @return Spark DataFrame containing the feature data.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no commit information was found
   *         for this feature group.
   * @throws IOException Generic IO exception.
   */
  @Override
  public Dataset<Row> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return read(false, readOptions);
  }

  /**
   * Reads the feature group from the offline or online storage as Spark DataFrame.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // define additional read options (this example applies to HUDI enabled FGs)
   *        Map<String, String> readOptions = new HashMap<String, String>() {{
   *                                                  put("hoodie.datasource.read.end.instanttime", "20230401211015")
   *                                                }};
   *        // read feature group data from online storage
   *        fg.read(true, readOptions)
   *        // read feature group data from offline storage
   *        fg.read(false, readOptions)
   * }
   * </pre>
   *
   * @param online Set `online` to `true` to read from the online storage.
   * @param readOptions Additional read options as key/value pairs.
   * @return Spark DataFrame containing the feature data.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no
   *                               commit information was found for this feature group.
   * @throws IOException  Generic IO exception.
   */
  @Override
  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return selectAll().read(online, readOptions);
  }

  /**
   * Reads Feature group into a dataframe at a specific point in time.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // read feature group data as of specific point in time (Hudi commit timestamp).
   *        fg.read("20230205210923")
   * }
   * </pre>
   *
   * @param wallclockTime Read data as of this point in time. Datetime string. The String should be formatted in one of
   *                      the following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return Spark DataFrame containing feature data.
   * @throws FeatureStoreException In case it's unable to identify format of the provided wallclockTime date format
   * @throws IOException  Generic IO exception.
   * @throws ParseException In case it's unable to parse provided wallclockTime to date type.
   */
  @Override
  public Dataset<Row> read(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime).read(false, null);
  }

  /**
   * Reads Feature group into a dataframe at a specific point in time.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // define additional read options (this example applies to HUDI enabled FGs)
   *        Map<String, String> readOptions = new HashMap<String, String>() {{
   *                                                  put("hoodie.datasource.read.end.instanttime", "20230401211015")
   *                                                }};
   *        // read feature group data as of specific point in time (Hudi commit timestamp).
   *        fg.read("20230205210923", readOptions)
   * }
   * </pre>
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param readOptions Additional read options as key-value pairs.
   * @return Spark DataFrame containing feature data.
   * @throws FeatureStoreException In case it's unable to identify format of the provided wallclockTime date format
   * @throws IOException  Generic IO exception.
   * @throws ParseException In case it's unable to parse provided wallclockTime to date type.
   */
  @Override
  public Dataset<Row> read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime).read(false, readOptions);
  }

  @Deprecated
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, null);
  }

  @Deprecated
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, readOptions);
  }

  /**
   * Get Query object to retrieve all features of the group at a point in the past.
   * This method selects all features in the feature group and returns a Query object
   * at the specified point in time. This can then either be read into a Dataframe
   * or used further to perform joins or construct a training dataset.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // get query object to retrieve feature group feature data as of
   *        // specific point in time (Hudi commit timestamp).
   *        fg.asOf("20230205210923")
   * }
   * </pre>
   *
   * @param wallclockTime Read data as of this point in time. Datetime string. The String should be formatted in one of
   *                      the following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException In case it's unable to identify format of the provided wallclockTime date format
   * @throws ParseException In case it's unable to parse provided wallclockTime to date type.
   */
  @Override
  public Query asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return selectAll().asOf(wallclockTime);
  }

  /**
   * Get Query object to retrieve all features of the group at a point in the past.
   * This method selects all features in the feature group and returns a Query object
   * at the specified point in time. This can then either be read into a Dataframe
   * or used further to perform joins or construct a training dataset.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // get query object to retrieve feature group feature data as of specific point in time "20230205210923"
   *        // but exclude commits until "20230204073411"  (Hudi commit timestamp).
   *        fg.asOf("20230205210923", "20230204073411")
   * }
   * </pre>
   *
   * @param wallclockTime Read data as of this point in time. Datetime string. The String should be formatted in one of
   *                      the following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param excludeUntil Exclude commits until this point in time. Datetime string. The String should be formatted in
   *                     one of the following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException In case it's unable to identify format of the provided wallclockTime date format.
   * @throws ParseException In case it's unable to parse provided wallclockTime to date type.
   */
  @Override
  public Query asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    return selectAll().asOf(wallclockTime, excludeUntil);
  }

  /**
   * Show the first `n` rows of the feature group.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // show top 5 lines of feature group data.
   *        fg.show(5);
   * }
   * </pre>
   *
   * @param numRows Number of rows to show.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no commit information was found
   *                               for this feature group;
   * @throws IOException Generic IO exception.
   */
  @Override
  public void show(int numRows) throws FeatureStoreException, IOException {
    show(numRows, false);
  }

  /**
   * Show the first `n` rows of the feature group.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // show top 5 lines of feature data from online storage.
   *        fg.show(5, true);
   * }
   * </pre>
   *
   * @param numRows Number of rows to show.
   * @param online If `true` read from online feature store.
   * @throws FeatureStoreException In case it cannot run read query on storage and/or no commit information was found
   *                               for this feature group;
   * @throws IOException Generic IO exception.
   */
  @Override
  public void show(int numRows, boolean online) throws FeatureStoreException, IOException {
    read(online).show(numRows);
  }

  /*
   * @deprecated
   * Save method is deprecated. Use insert method instead.
   */
  @Deprecated
  public void save(Dataset<Row> featureData) throws FeatureStoreException, IOException, ParseException {
    save(featureData, null);
  }

  /*
   * @deprecated
   * Save method is deprecated. Use insert method instead.
   */
  @Deprecated
  public void save(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.save(this, featureData, partitionKeys, hudiPrecombineKey,
        writeOptions);
    codeEngine.saveCode(this);
    if (statisticsConfig.getEnabled()) {
      statisticsEngine.computeStatistics(this, featureData, null);
    }
  }

  /**
   * Incrementally insert data to a feature group or overwrite all  data contained in the feature group.
   * The `features` dataframe can be a Spark DataFrame or RDD.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        //insert feature data
   *        fg.insert(featureData);
   * }
   * </pre>
   *
   * @param featureData spark DataFrame, RDD. Features to be saved.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void insert(Dataset<Row> featureData) throws IOException, FeatureStoreException, ParseException {
    insert(featureData, null, false);
  }

  /**
   * Incrementally insert data to a feature group or overwrite all  data contained in the feature group.
   * The `features` dataframe can be a Spark DataFrame or RDD.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // Define additional write options (this example applies to HUDI enabled FGs)
   *        Map<String, String> writeOptions = = new HashMap<String, String>() {{
   *                           put("hoodie.bulkinsert.shuffle.parallelism", "5");
   *                           put("hoodie.insert.shuffle.parallelism", "5");
   *                           put("hoodie.upsert.shuffle.parallelism", "5");}
   *                           };
   *        // insert feature data
   *        fg.insert(featureData, writeOptions);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param writeOptions Additional write options as key-value pairs.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void insert(Dataset<Row> featureData,  Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, false, null, writeOptions);
  }

  /**
   * Incrementally insert data to a feature group or overwrite all data contained in the feature group. By default,
   * the data is inserted into the offline storage as well as the online storage if the feature group is
   * online enabled. To insert only into the online or offline storage specify Storage.ONLINE or Storage.OFFLINE
   * respectively. The `features` dataframe can be a Spark DataFrame or RDD.
   * If statistics are enabled, statistics are recomputed for the entire feature group. If feature group's time travel
   * format is `HUDI` then `operation` argument can be either `insert` or `upsert`.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // insert feature data in offline only
   *        fg.insert(featureData, Storage.OFFLINE);
   *
   *        // Or insert feature data in online only
   *        fg.insert(featureData, Storage.ONLINE);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param storage Overwrite default behaviour, write to offline storage only with `Storage.OFFLINE` or online only
   *                with `Storage.ONLINE`
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void insert(Dataset<Row> featureData, Storage storage)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, storage, false, null, null);
  }

  /**
   * Incrementally insert data to a feature group or overwrite all  data contained in the feature group. By default,
   * the data is inserted into the offline storage as well as the online storage if the feature group is
   * online enabled. To insert only into the online or offline storage specify Storage.ONLINE or Storage.OFFLINE
   * respectively. The `features` dataframe can be a Spark DataFrame or RDD.
   * If statistics are enabled, statistics are recomputed for the entire feature group. If feature group's time travel
   * format is `HUDI` then `operation` argument can be either `insert` or `upsert`.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // insert feature data and drop all data in the feature group before inserting new data
   *        fg.insert(featureData, true);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param overwrite Drop all data in the feature group before inserting new data. This does not affect metadata.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void insert(Dataset<Row> featureData, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, null, overwrite);
  }

  /**
   * Incrementally insert data to a feature group or overwrite all  data contained in the feature group. By default,
   * the data is inserted into the offline storage as well as the online storage if the feature group is
   * online enabled. To insert only into the online or offline storage specify Storage.ONLINE or Storage.OFFLINE
   * respectively. The `features` dataframe can be a Spark DataFrame or RDD.
   * If statistics are enabled, statistics are recomputed for the entire feature group. If feature group's time travel
   * format is `HUDI` then `operation` argument can be either `insert` or `upsert`.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // insert feature data in offline only and drop all data in the feature group before inserting new data
   *        fg.insert(featureData, Storage.OFFLINE, true);
   *
   *        // Or insert feature data in online only and drop all data in the feature group before inserting new data
   *        fg.insert(featureData, Storage.ONLINE, true);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param storage Overwrite default behaviour, write to offline storage only with `Storage.OFFLINE` or online only
   *                with `Storage.ONLINE`.
   * @param overwrite Drop all data in the feature group before inserting new data. This does not affect metadata.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, storage, overwrite, null, null);
  }

  /**
   * Incrementally insert data to a feature group or overwrite all  data contained in the feature group. By default,
   * the data is inserted into the offline storage as well as the online storage if the feature group is
   * online enabled. To insert only into the online or offline storage specify Storage.ONLINE or Storage.OFFLINE
   * respectively. The `features` dataframe can be a Spark DataFrame or RDD.
   * If statistics are enabled, statistics are recomputed for the entire feature group. If feature group's time travel
   * format is `HUDI` then `operation` argument can be either `insert` or `upsert`.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // Define additional write options (this example applies to HUDI enabled FGs)
   *        Map<String, String> writeOptions = = new HashMap<String, String>() {{
   *                           put("hoodie.bulkinsert.shuffle.parallelism", "5");
   *                           put("hoodie.insert.shuffle.parallelism", "5");
   *                           put("hoodie.upsert.shuffle.parallelism", "5");}
   *                           };
   *        // insert feature data and drop all data in the feature group before inserting new data
   *        fg.insert(featureData, true, writeOptions);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param overwrite Drop all data in the feature group before inserting new data. This does not affect metadata.
   * @param writeOptions Additional write options as key-value pairs.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, overwrite, null, writeOptions);
  }

  /**
   * Incrementally insert data to a feature group or overwrite all  data contained in the feature group. By default,
   * the data is inserted into the offline storage as well as the online storage if the feature group is
   * online enabled. To insert only into the online or offline storage specify Storage.ONLINE or Storage.OFFLINE
   * respectively. The `features` dataframe can be a Spark DataFrame or RDD.
   * If statistics are enabled, statistics are recomputed for the entire feature group. If feature group's time travel
   * format is `HUDI` then `operation` argument can be either `insert` or `upsert`.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // Define additional write options (this example applies to HUDI enabled FGs)
   *        Map<String, String> writeOptions = = new HashMap<String, String>() {{
   *                           put("hoodie.bulkinsert.shuffle.parallelism", "5");
   *                           put("hoodie.insert.shuffle.parallelism", "5");
   *                           put("hoodie.upsert.shuffle.parallelism", "5");}
   *                           };
   *        // insert feature data
   *        fg.insert(featureData, HudiOperationType.INSERT);
   *
   *        // upsert feature data
   *        fg.insert(featureData, HudiOperationType.UPSERT);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param operation   commit operation type, INSERT or UPSERT.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void insert(Dataset<Row> featureData, HudiOperationType operation)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, false, operation, null);
  }

  /**
   * Incrementally insert data to a feature group or overwrite all  data contained in the feature group. By default,
   * the data is inserted into the offline storage as well as the online storage if the feature group is
   * online enabled. To insert only into the online or offline storage specify Storage.ONLINE or Storage.OFFLINE
   * respectively. The `features` dataframe can be a Spark DataFrame or RDD.
   * If statistics are enabled, statistics are recomputed for the entire feature group. If feature group's time travel
   * format is `HUDI` then `operation` argument can be either `insert` or `upsert`.
   * If the feature group doesn't exist, the insert method will create the necessary metadata the first time it is
   * invoked and write the specified `features` dataframe as feature group to the online/offline feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // Define additional write options (this example applies to HUDI enabled FGs)
   *        Map<String, String> writeOptions = = new HashMap<String, String>() {{
   *                           put("hoodie.bulkinsert.shuffle.parallelism", "5");
   *                           put("hoodie.insert.shuffle.parallelism", "5");
   *                           put("hoodie.upsert.shuffle.parallelism", "5");}
   *                           };
   *        // insert feature data in offline only with additional write options and drop all previous data before new
   *        // data is inserted
   *        fg.insert(featureData, Storage.OFFLINE, true, HudiOperationType.INSERT, writeOptions);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Features to be saved.
   * @param storage Overwrite default behaviour, write to offline storage only with `Storage.OFFLINE` or online only
   *                with `Storage.ONLINE`.
   * @param overwrite Drop all data in the feature group before inserting new data. This does not affect metadata.
   * @param operation   commit operation type, INSERT or UPSERT.
   * @param writeOptions Additional write options as key-value pairs.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If client is not connected to Hopsworks; cannot run read query on storage and/or
   *                               can't reconcile HUDI schema.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite, HudiOperationType operation,
                     Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {

    // operation is only valid for time travel enabled feature group
    if (operation != null && this.timeTravelFormat == TimeTravelFormat.NONE) {
      throw new IllegalArgumentException("operation argument is valid only for time travel enable feature groups");
    }

    if (operation == null && this.timeTravelFormat == TimeTravelFormat.HUDI) {
      if (overwrite) {
        operation = HudiOperationType.BULK_INSERT;
      } else {
        operation = HudiOperationType.UPSERT;
      }
    }

    featureGroupEngine.insert(this, featureData, storage, operation,
        overwrite ? SaveMode.Overwrite : SaveMode.Append, partitionKeys, hudiPrecombineKey, writeOptions);

    codeEngine.saveCode(this);
    computeStatistics();
  }

  @Override
  public void insert(Dataset<Row> featureData, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {
    new FeatureStoreException("This method is not implemented for FeatureGroup class");
  }

  @Override
  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions,
                     JobConfiguration jobConfiguration) throws FeatureStoreException, IOException, ParseException {
    new FeatureStoreException("This method is not implemented for FeatureGroup class");
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @param featureData Spark dataframe containing feature data
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  public StreamingQuery insertStream(Dataset<Row> featureData)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, null, null, false, null, null, null);
  }

  @Deprecated
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, null, false, null, null, null);
  }

  @Deprecated
  @Override
  public StreamingQuery insertStream(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException, ParseException {
    return insertStream(featureData, null, null, false, null, null, writeOptions);
  }

  @Override
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, null, false, null, null, writeOptions);
  }

  @Deprecated
  @Override
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, outputMode, false, null, null, null);
  }

  @Deprecated
  @Override
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     String checkpointLocation)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, outputMode, false, null, checkpointLocation, null);
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @param featureData Spark dataframe containing feature data
   * @param queryName name of spark StreamingQuery
   * @param outputMode outputMode
   * @param awaitTermination whether or not to wait for query Termination
   * @param timeout timeout
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  @Override
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, null, null);
  }

  @Deprecated
  @Override
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout, String checkpointLocation)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, checkpointLocation, null);
  }

  /**
   * insert streaming dataframe in the Feature group.
   *
   * @param featureData Spark dataframe containing feature data
   * @param queryName name of spark StreamingQuery
   * @param outputMode outputMode
   * @param awaitTermination whether or not to wait for query Termination
   * @param checkpointLocation path to checkpoint location directory
   * @return StreamingQuery
   * @throws StreamingQueryException StreamingQueryException
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   * @throws TimeoutException TimeoutException
   * @throws ParseException ParseException
   * @deprecated
   * insertStream method is deprecated FeatureGroups. Full capability insertStream is available for StreamFeatureGroups.
   */
  @Deprecated
  @Override
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, String checkpointLocation)
      throws StreamingQueryException, IOException, FeatureStoreException, TimeoutException, ParseException {
    return insertStream(featureData, queryName, outputMode, awaitTermination, null, checkpointLocation, null);
  }

  @Deprecated
  @Override
  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout, String checkpointLocation,
                                     Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException, ParseException {
    if (!featureData.isStreaming()) {
      throw new FeatureStoreException(
          "Features have to be a streaming type spark dataframe. Use `insert()` method instead.");
    }
    LOGGER.info("StatisticsWarning: Stream ingestion for feature group `" + name + "`, with version `" + version
        + "` will not compute statistics.");

    return featureGroupEngine.insertStream(this, featureData, queryName, outputMode, awaitTermination,
        timeout, checkpointLocation, partitionKeys, hudiPrecombineKey, writeOptions);
  }

  @Override
  public Object insertStream(Dataset<Row> featureData, String queryName, String outputMode, boolean awaitTermination,
                             Long timeout, String checkpointLocation, Map<String, String> writeOptions,
                             JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException, ParseException {
    return null;
  }

  /**
   * Drops records present in the provided DataFrame and commits it as update to this Feature group. This method can
   * only be used on time travel enabled feature groups.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // Drops records of feature data and commit
   *        fg.commitDeleteRecord(featureData);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Feature data to be deleted.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void commitDeleteRecord(Dataset<Row> featureData)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.commitDelete(this, featureData, null);
  }

  /**
   * Drops records present in the provided DataFrame and commits it as update to this Feature group. This method can
   * only be used on time travel enabled feature groups.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // Define additional write options (this example applies to HUDI enabled FGs)
   *        Map<String, String> writeOptions = = new HashMap<String, String>() {{
   *                           put("hoodie.bulkinsert.shuffle.parallelism", "5");
   *                           put("hoodie.insert.shuffle.parallelism", "5");
   *                           put("hoodie.upsert.shuffle.parallelism", "5");}
   *                           };
   *        // Drops records of feature data and commit
   *        fg.commitDeleteRecord(featureData, writeOptions);
   * }
   * </pre>
   *
   * @param featureData Spark DataFrame, RDD. Feature data to be deleted.
   * @param writeOptions Additional write options as key-value pairs.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public void commitDeleteRecord(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.commitDelete(this, featureData, writeOptions);
  }

  /**
   * Retrieves commit timeline for this feature group. This method can only be used on time travel enabled feature
   * groups.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // get commit timeline.
   *        fg.commitDetails();
   * }
   * </pre>
   *
   * @return commit details.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, null);
  }

  /**
   * Retrieves commit timeline for this feature group. This method can only be used on time travel enabled feature
   * groups.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // get latest 10 commit details.
   *        fg.commitDetails(10);
   * }
   * </pre>
   *
   * @param limit number of commits to return.
   * @return commit details.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, limit);
  }

  /**
   * Return commit details as of specific point in time.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // get commit details as of 20230206
   *        fg.commitDetails("20230206");
   * }
   * </pre>
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return commit details.
   * @throws FeatureStoreException If Client is not connected to Hopsworks, unable to identify format of the
   *                               provided wallclockTime date format and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, null);
  }

  /**
   * Return commit details as of specific point in time.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // get top 10 commit details as of 20230206
   *        fg.commitDetails("20230206", 10);
   * }
   * </pre>
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   *
   * @param limit number of commits to return.
   * @return commit details.
   * @throws FeatureStoreException If Client is not connected to Hopsworks, unable to identify format of the
   *                               provided wallclockTime date format and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI commit date string to date type.
   */
  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, limit);
  }

  /**
   * Select a subset of features of the feature group and return a query object. The query can be used to construct
   * joins of feature groups or create a feature view with a subset of features of the feature group.
   * @param features List of Feature meta data objects.
   * @return Query object.
   */
  @Override
  public Query selectFeatures(List<Feature> features) {
    return new Query(this, features);
  }

  /**
   * Select a subset of features of the feature group and return a query object. The query can be used to construct
   * joins of feature groups or create a feature view with a subset of features of the feature group.
   * @param features List of Feature names.
   * @return Query object.
   */
  @Override
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
  @Override
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
  @Override
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
  @Override
  public Query selectExcept(List<String> features) {
    return new Query(this,
        getFeatures().stream().filter(f -> !features.contains(f.getName())).collect(Collectors.toList()));
  }

  /**
   * Update the metadata of multiple features.
   * Currently only feature description updates are supported.
   *
   * @param features List of Feature metadata objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks, unable to identify date format and/or
   *                               no commit information was found for this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse date string to date type.
   */
  @Override
  public void updateFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.appendFeatures(this, features, this.getClass());
  }

  /**
   * Update the metadata of feature.
   * Currently only feature description updates are supported.
   *
   * @param feature Feature metadata object
   * @throws FeatureStoreException If Client is not connected to Hopsworks, unable to identify date format and/or
   *                               no commit information was found for this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse date string to date type.
   */
  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.appendFeatures(this, Collections.singletonList(feature), this.getClass());
  }

  /**
   * Append features to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features list of Feature metadata objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks, unable to identify date format and/or
   *                               no commit information was found for this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse date string to date type.
   */
  @Override
  public void appendFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.appendFeatures(this, new ArrayList<>(features), this.getClass());
  }

  /**
   * Append a single feature to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features List of Feature metadata objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks, unable to identify date format and/or
   *                               no commit information was found for this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse date string to date type.
   */
  @Override
  public void appendFeatures(Feature features) throws FeatureStoreException, IOException, ParseException {
    List<Feature> featureList = new ArrayList<>();
    featureList.add(features);
    featureGroupEngine.appendFeatures(this, featureList, this.getClass());
  }

  /**
   * Recompute the statistics for the feature group and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException If Client is not connected to Hopsworks,
   * @throws IOException Generic IO exception.
   */
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

  /**
   * Recompute the statistics for the feature group and save them to the feature store. Statistics are only computed
   * for data in the offline storage of the feature group.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        // get feature group handle
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   *        // compute statistics as of 20230206
   *        fg.computeStatistics("20230206", 10);
   * }
   * </pre>
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `yyyyMMdd`, `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return statistics object of computed statistics
   * @throws FeatureStoreException In case Client is not connected to Hopsworks, unable to identify format of the
   *                               provided wallclockTime date format and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse HUDI and or statistics commit date string to date type.
   */
  @Override
  public Statistics computeStatistics(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    if (statisticsConfig.getEnabled()) {
      Map<Long, Map<String, String>> latestCommitMetaData =
          featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, 1);
      Dataset<Row> featureData = selectAll().asOf(wallclockTime).read(false, null);
      Long commitId = (Long) latestCommitMetaData.keySet().toArray()[0];
      return statisticsEngine.computeStatistics(this, featureData, commitId);
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
   * @throws FeatureStoreException In case Client is not connected to Hopsworks, unable to identify format of the
   *                               provided wallclockTime date format and/or no commit information was found for
   *                               this feature group;
   * @throws IOException Generic IO exception.
   */
  @JsonIgnore
  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    return statisticsEngine.getLast(this);
  }
}
