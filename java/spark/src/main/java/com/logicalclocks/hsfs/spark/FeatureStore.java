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

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.spark.constructor.Query;
import com.logicalclocks.hsfs.spark.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.spark.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.TrainingDatasetBase;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;

import lombok.NonNull;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FeatureStore extends FeatureStoreBase<Query> {

  private FeatureGroupEngine featureGroupEngine;
  private FeatureViewEngine featureViewEngine;

  public FeatureStore() {
    trainingDatasetApi = new TrainingDatasetApi();
    storageConnectorApi = new StorageConnectorApi();
    featureViewEngine = new FeatureViewEngine();
    featureGroupEngine = new FeatureGroupEngine();
  }

  /**
   * Get a feature group object from the feature store.
   *
   * <p>Feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices", 1);
   * }
   * </pre>
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup The feature group metadata object.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  public FeatureGroup getFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupEngine.getFeatureGroup(this, name, version);
  }

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * <p>Feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getFeatureGroup("electricity_prices");
   * }
   * </pre>
   *
   * @param name the name of the feature group
   * @return FeatureGroup The feature group metadata object.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  public FeatureGroup getFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + FeatureStoreBase.DEFAULT_VERSION + "`.");
    return getFeatureGroup(name, FeatureStoreBase.DEFAULT_VERSION);
  }

  /**
   * Get a list of all versions of a feature group from the feature store.
   *
   * <p>Feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getFeatureGroups("electricity_prices");
   * }
   * </pre>
   *
   * @param name the name of the feature group
   * @return List of FeatureGroup metadata objects.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  public scala.collection.Seq<FeatureGroup> getFeatureGroups(@NonNull String name)
      throws FeatureStoreException, IOException {
    List<FeatureGroup> featureGroups = new ArrayList<>();
    for (FeatureGroupBase fg: featureGroupEngine.getFeatureGroups(this, name)) {
      featureGroups.add((FeatureGroup) fg);
    }
    return JavaConverters.asScalaBufferConverter(featureGroups)
        .asScala().toSeq();
  }

  /**
   * Create feature group builder object.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup.FeatureGroupBuilder = fs.createFeatureGroup()
   * }
   * </pre>
   *
   * @return FeatureGroup.FeatureGroupBuilder a FeatureGroup builder object.
   */
  public FeatureGroup.FeatureGroupBuilder createFeatureGroup() {
    return FeatureGroup.builder()
        .featureStore(this);
  }

  /**
   * Get feature group metadata object or create a new one if it doesn't exist.
   * This method doesn't update existing feature group metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getOrCreateFeatureGroup("fg_name", 1);
   * }
   * </pre>
   *
   * @param name of the feature group to retrieve or create.
   * @param version of the feature group to retrieve or create.
   * @return FeatureGroup The feature group metadata object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If unable to retrieve FeatureGroup from the feature store.
   */
  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version) throws IOException, FeatureStoreException {
    return featureGroupEngine.getOrCreateFeatureGroup(this, name, version, null, null,
        null, null, false, null, null, null, null, null);
  }

  /**
   * Get feature group metadata object or create a new one if it doesn't exist.
   * This method doesn't update existing feature group metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getOrCreateFeatureGroup("fg_name", 1, primaryKeys, true, "datetime");
   * }
   * </pre>
   *
   * @param name of the feature group to retrieve or create.
   * @param version of the feature group to retrieve or create.
   * @param primaryKeys  A list of feature names to be used as primary key for the
   *                     feature group. This primary key can be a composite key of multiple
   *                     features and will be used as joining key.
   * @param onlineEnabled Define whether the feature group should be made available also in the online feature store
   *                      for low latency access.
   * @param eventTime Name of the feature containing the event time for the features in this feature group. If
   *                  eventTime is set the feature group can be used for point-in-time joins.
   *                  The supported data types for the eventTime column are: timestamp, date and bigint
   * @return FeatureGroup: The feature group metadata object
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If unable to retrieve FeatureGroup from the feature store.
   */
  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                              boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException {
    return featureGroupEngine.getOrCreateFeatureGroup(this, name, version, null, primaryKeys,
        null, null, onlineEnabled, null, null, null, null, eventTime);
  }

  /**
   * Get feature group metadata object or create a new one if it doesn't exist.
   * This method doesn't update existing feature group metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getOrCreateFeatureGroup("fg_name", 1, primaryKeys, partitionKeys, true, "datetime");
   * }
   * </pre>
   *
   * @param name of the feature group to retrieve or create.
   * @param version of the feature group to retrieve or create.
   * @param primaryKeys  A list of feature names to be used as primary key for the
   *                     feature group. This primary key can be a composite key of multiple
   *                     features and will be used as joining key.
   * @param partitionKeys A list of feature names to be used as partition key when writing the feature data to the
   *                      offline storage.
   * @param onlineEnabled Define whether the feature group should be made available also in the online feature store
   *                      for low latency access.
   * @param eventTime Name of the feature containing the event time for the features in this feature group. If
   *                  eventTime is set the feature group can be used for point-in-time joins.
   *                  The supported data types for the eventTime column are: timestamp, date and bigint
   * @return FeatureGroup: The feature group metadata object
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If unable to retrieve FeatureGroup from the feature store.
   */
  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version,
                                              List<String> primaryKeys,
                                              List<String> partitionKeys,
                                              boolean onlineEnabled,
                                              String eventTime) throws IOException, FeatureStoreException {

    return featureGroupEngine.getOrCreateFeatureGroup(this, name, version, null, primaryKeys,
        partitionKeys, null, onlineEnabled, null, null, null, null, eventTime);
  }

  /**
   * Get feature group metadata object or create a new one if it doesn't exist.
   * This method doesn't update existing feature group metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getOrCreateFeatureGroup("fg_name", 1, primaryKeys, partitionKeys, true, "datetime");
   * }
   * </pre>
   *
   * @param name of the feature group to retrieve or create.
   * @param version of the feature group to retrieve or create.
   * @param description contents of the feature group to improve discoverability for Data Scientists
   * @param primaryKeys  A list of feature names to be used as primary key for the
   *                     feature group. This primary key can be a composite key of multiple
   *                     features and will be used as joining key.
   * @param partitionKeys A list of feature names to be used as partition key when writing the feature data to the
   *                      offline storage.
   * @param hudiPrecombineKey A feature name to be used as a precombine key for the `HUDI` feature group.  If feature
   *                          group has time travel format `HUDI` and hudi precombine key was not specified then
   *                          the first primary key of the feature group will be used as hudi precombine key.
   * @param onlineEnabled Define whether the feature group should be made available also in the online feature store
   *                      for low latency access.
   * @param timeTravelFormat  Format used for time travel: `TimeTravelFormat.HUDI` or `TimeTravelFormat.NONE`.
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=false`.
   * @param topicName Optionally, define the name of the topic used for data ingestion. If left undefined it defaults
   *                  to using project topic.
   * @param notificationTopicName Optionally, define the name of the topic used for sending notifications when entries
   *                  are inserted or updated on the online feature store. If left undefined no notifications are sent.
   * @param eventTime Name of the feature containing the event time for the features in this feature group. If
   *                  eventTime is set the feature group can be used for point-in-time joins.
   *                  The supported data types for the eventTime column are: timestamp, date and bigint
   * @return FeatureGroup: The feature group metadata object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If unable to retrieve FeatureGroup from the feature store.
   */
  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version, String description,
                                              List<String> primaryKeys, List<String> partitionKeys,
                                              String hudiPrecombineKey, boolean onlineEnabled,
                                              TimeTravelFormat timeTravelFormat, StatisticsConfig statisticsConfig,
                                              String topicName, String notificationTopicName, String eventTime)
      throws IOException, FeatureStoreException {

    return featureGroupEngine.getOrCreateFeatureGroup(this, name, version, description, primaryKeys,
        partitionKeys, hudiPrecombineKey, onlineEnabled, timeTravelFormat, statisticsConfig, topicName,
        notificationTopicName, eventTime);
  }

  /**
   * Get a stream feature group object with default version `1` from the feature store.
   *
   * <p>Getting a stream feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getStreamFeatureGroup("electricity_prices");
   * <}
   * </pre>
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + FeatureStoreBase.DEFAULT_VERSION + "`.");
    return getStreamFeatureGroup(name, FeatureStoreBase.DEFAULT_VERSION);
  }

  /**
   * Get a stream feature group object from the feature store.
   *
   * <p>Getting a stream feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getStreamFeatureGroup("electricity_prices", 1);
   * }
   * </pre>
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @return StreamFeatureGroup The stream feature group metadata object.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupEngine.getStreamFeatureGroup(this, name, version);
  }

  @Override
  public StreamFeatureGroup createStreamFeatureGroup(@NonNull String name, Integer version, String description,
                                                   Boolean onlineEnabled, TimeTravelFormat timeTravelFormat,
                                                   List<String> primaryKey, List<String> partitionKey, String eventTime,
                                                   String hudiPrecombineKey, List<Feature> features,
                                                   StatisticsConfig statisticsConfig, StorageConnector storageConnector,
                                                   String path) {
    return StreamFeatureGroup.builder()
        .featureStore(this)
        .name(name)
        .version(version)
        .description(description)
        .onlineEnabled(onlineEnabled)
        .timeTravelFormat(timeTravelFormat)
        .primaryKeys(primaryKey)
        .partitionKeys(partitionKey)
        .eventTime(eventTime)
        .hudiPrecombineKey(hudiPrecombineKey)
        .features(features)
        .statisticsConfig(statisticsConfig)
        .storageConnector(storageConnector)
        .path(path)
        .build();
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(@NonNull String name,
                                                          Integer version,
                                                          String description,
                                                          Boolean onlineEnabled,
                                                          TimeTravelFormat timeTravelFormat,
                                                          List<String> primaryKeys,
                                                          List<String> partitionKeys,
                                                          String eventTime,
                                                          String hudiPrecombineKey,
                                                          List<Feature> features,
                                                          StatisticsConfig statisticsConfig,
                                                          StorageConnector storageConnector,
                                                          String path) throws IOException, FeatureStoreException {
    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, description,
        primaryKeys, partitionKeys, hudiPrecombineKey, onlineEnabled, statisticsConfig, eventTime, timeTravelFormat,
        features, storageConnector, path);
  }

  /**
   * Get stream feature group metadata object or create a new one if it doesn't exist.
   * This method doesn't update existing feature group metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getOrCreateStreamFeatureGroup("fg_name", 1);
   * }
   * </pre>
   *
   * @param name of the feature group to retrieve or create.
   * @param version of the feature group to retrieve or create.
   * @return FeatureGroup The feature group metadata object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If unable to retrieve StreamFeatureGroup from the feature store.
   */
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version)
      throws IOException, FeatureStoreException {
    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, null,
        null, null, null, false, null, null, null, null, null, null);
  }

  /**
   * Get stream feature group metadata object or create a new one if it doesn't exist.
   * This method doesn't update existing feature group metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getOrCreateStreamFeatureGroup("fg_name", 1, primaryKeys, true, "datetime");
   * }
   * </pre>
   *
   * @param name Name of the feature group to retrieve or create.
   * @param version Version of the feature group to retrieve or create.
   * @param primaryKeys  A list of feature names to be used as primary key for the
   *                     feature group. This primary key can be a composite key of multiple
   *                     features and will be used as joining key.
   * @param onlineEnabled Define whether the feature group should be made available also in the online feature store
   *                      for low latency access.
   * @param eventTime Name of the feature containing the event time for the features in this feature group. If
   *                  eventTime is set the feature group can be used for point-in-time joins.
   *                  The supported data types for the eventTime column are: timestamp, date and bigint
   * @return FeatureGroup The feature group metadata object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If unable to retrieve StreamFeatureGroup from the feature store.
   */
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException {
    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, null,
        primaryKeys, null, null, onlineEnabled, null, eventTime, null, null, null, null);
  }

  /**
   * Get stream feature group metadata object or create a new one if it doesn't exist.
   * This method doesn't update existing feature group metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getOrCreateStreamFeatureGroup("fg_name", 1, primaryKeys, partitionKeys, true,
   *        "datetime");
   * }
   * </pre>
   *
   * @param name Name of the feature group to retrieve or create.
   * @param version Version of the feature group to retrieve or create.
   * @param primaryKeys  A list of feature names to be used as primary key for the
   *                     feature group. This primary key can be a composite key of multiple
   *                     features and will be used as joining key.
   * @param partitionKeys A list of feature names to be used as partition key when writing the feature data to the
   *                      offline storage.
   * @param onlineEnabled Define whether the feature group should be made available also in the online feature store
   *                      for low latency access.
   * @param eventTime Name of the feature containing the event time for the features in this feature group. If
   *                  eventTime is set the feature group can be used for point-in-time joins.
   *                  The supported data types for the eventTime column are: timestamp, date and bigint
   * @return FeatureGroup: The feature group metadata object
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If unable to retrieve StreamFeatureGroup from the feature store.
   */
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          List<String> partitionKeys, boolean onlineEnabled,
                                                          String eventTime) throws IOException, FeatureStoreException {


    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, null,
        primaryKeys, partitionKeys, null, onlineEnabled, null, eventTime, null, null, null, null);
  }

  /**
   * Get stream feature group metadata object or create a new one if it doesn't exist.
   * This method doesn't update existing feature group metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StreamFeatureGroup fg = fs.getOrCreateStreamFeatureGroup("fg_name", 1, primaryKeys,
   *        partitionKeys, true, "datetime");
   * }
   * </pre>
   *
   * @param name of the feature group to retrieve or create.
   * @param version of the feature group to retrieve or create.
   * @param description contents of the feature group to improve discoverability for Data Scientists
   * @param primaryKeys  A list of feature names to be used as primary key for the
   *                     feature group. This primary key can be a composite key of multiple
   *                     features and will be used as joining key.
   * @param partitionKeys A list of feature names to be used as partition key when writing the feature data to the
   *                      offline storage.
   * @param hudiPrecombineKey A feature name to be used as a precombine key for the `HUDI` feature group.  If feature
   *                          group has time travel format `HUDI` and hudi precombine key was not specified then
   *                          the first primary key of the feature group will be used as hudi precombine key.
   * @param onlineEnabled Define whether the feature group should be made available also in the online feature store
   *                      for low latency access.
   * @param timeTravelFormat Format used for time travel, defaults to `"HUDI"`.
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=false`.
   * @param eventTime Name of the feature containing the event
   *                 time for the features in this feature group. If eventTime is set
   *                 the feature group can be used for point-in-time joins.
   * @return FeatureGroup: The feature group metadata object.
   * @throws IOException Generic IO exception.
   * @throws FeatureStoreException If unable to retrieve FeatureGroup from the feature store.
   */
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, String description,
                                                          List<String> primaryKeys, List<String> partitionKeys,
                                                          String hudiPrecombineKey, boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig,
                                                          String eventTime, TimeTravelFormat timeTravelFormat)
      throws IOException, FeatureStoreException {
    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, description,
        primaryKeys, partitionKeys, hudiPrecombineKey, onlineEnabled, statisticsConfig, eventTime, timeTravelFormat,
        null, null, null);
  }

  /**
   * Create external feature group builder object.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        ExternalFeatureGroup.ExternalFeatureGroupBuilder = fs.createExternalFeatureGroup()
   * }
   * </pre>
   *
   * @return ExternalFeatureGroup.ExternalFeatureGroupBuilder a ExternalFeatureGroup builder object.
   */
  public ExternalFeatureGroup.ExternalFeatureGroupBuilder createExternalFeatureGroup() {
    return ExternalFeatureGroup.builder()
        .featureStore(this);
  }

  @Deprecated
  public ExternalFeatureGroup.ExternalFeatureGroupBuilder createOnDemandFeatureGroup() {
    return ExternalFeatureGroup.builder()
        .featureStore(this);
  }


  /**
   * Get a list of all versions of an external feature group from the feature store.
   *
   * <p>Feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getExternalFeatureGroups("external_fg_name");
   * }
   * </pre>
   *
   * @param name The name of the feature group.
   * @return List of ExternalFeatureGroup metadata objects.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  public scala.collection.Seq<ExternalFeatureGroup> getExternalFeatureGroups(@NonNull String name)
      throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(featureGroupEngine.getExternalFeatureGroups(this, name))
        .asScala().toSeq();
  }

  /**
   * Get an external feature group object from the feature store.
   *
   * <p>Getting a stream feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        ExternalFeatureGroup fg = fs.getExternalFeatureGroup("external_fg_name", 1);
   * }
   * </pre>
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return ExternalFeatureGroup The external feature group metadata object.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  public ExternalFeatureGroup getExternalFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupEngine.getExternalFeatureGroup(this, name, version);
  }

  /**
   * Get an external feature group object with default version `1` from the feature store.
   *
   * <p>Getting external feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        ExternalFeatureGroup fg = fs.getExternalFeatureGroup("external_fg_name");
   * }
   * </pre>
   *
   * @param name the name of the feature group
   * @return ExternalFeatureGroup The external feature group metadata object.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  public ExternalFeatureGroup getExternalFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + FeatureStoreBase.DEFAULT_VERSION + "`.");
    return getExternalFeatureGroup(name, FeatureStoreBase.DEFAULT_VERSION);
  }

  @Deprecated
  public ExternalFeatureGroup getOnDemandFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupEngine.getExternalFeatureGroup(this, name, version);
  }

  @Deprecated
  public ExternalFeatureGroup getOnDemandFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + FeatureStoreBase.DEFAULT_VERSION + "`.");
    return getExternalFeatureGroup(name, FeatureStoreBase.DEFAULT_VERSION);
  }

  @Deprecated
  public scala.collection.Seq<ExternalFeatureGroup> getOnDemandFeatureGroups(@NonNull String name)
      throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(featureGroupEngine.getExternalFeatureGroups(this, name))
        .asScala().toSeq();
  }

  public FeatureView.FeatureViewBuilder createFeatureView() {
    return new FeatureView.FeatureViewBuilder(this);
  }

  /**
   * Get feature view metadata object or create a new one if it doesn't exist. This method doesn't update
   * existing feature view metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureView fv = fs.getOrCreateFeatureView("fv_name", query, 1);
   * }
   * </pre>
   *
   * @param name Name of the feature view.
   * @param query Query object.
   * @param version Version of the feature view.
   * @return FeatureView The feature view metadata object.
   * @throws FeatureStoreException If unable to retrieve FeatureView from the feature store.
   * @throws IOException Generic IO exception.
   */
  public FeatureView getOrCreateFeatureView(String name, Query query, Integer version)
      throws FeatureStoreException, IOException {
    return featureViewEngine.getOrCreateFeatureView(this, name, version, query, null, null);
  }

  /**
   * Get feature view metadata object or create a new one if it doesn't exist. This method doesn't update
   * existing feature view metadata.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureView fv = fs.getOrCreateFeatureView("fv_name", query, 1, description, labels);
   *        "datetime");
   * }
   * </pre>
   *
   * @param name Name of the feature view.
   * @param query Query object.
   * @param version Version of the feature view.
   * @param description Description of the feature view.
   * @param labels List of label features.
   * @return FeatureView The feature view metadata object.
   * @throws FeatureStoreException If unable to retrieve FeatureView from the feature store.
   * @throws IOException Generic IO exception.
   */
  public FeatureView getOrCreateFeatureView(String name, Query query, Integer version, String description,
                                            List<String> labels) throws FeatureStoreException, IOException {
    return featureViewEngine.getOrCreateFeatureView(this, name, version, query, description, labels);
  }

  /**
   * Get a feature view object from the selected feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   * }
   * </pre>
   *
   * @param name    Name of the feature view.
   * @param version Version to get.
   * @return FeatureView The feature view metadata object.
   * @throws FeatureStoreException If unable to retrieve FeatureView from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public FeatureView getFeatureView(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureViewEngine.get(this, name, version);
  }

  /**
   * Get a feature view object with the default version `1` from the selected feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureView fv = fs.getFeatureView("fv_name");
   * }
   * </pre>
   *
   * @param name Name of the feature view.
   * @return FeatureView The feature view metadata object.
   * @throws FeatureStoreException If unable to retrieve FeatureView from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public FeatureView getFeatureView(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature view `" + name + "`, defaulting to `"
        + FeatureStoreBase.DEFAULT_VERSION + "`.");
    return getFeatureView(name, FeatureStoreBase.DEFAULT_VERSION);
  }

  public Dataset<Row> sql(String query) {
    return SparkEngine.getInstance().sql(query);
  }

  @Deprecated
  public TrainingDataset.TrainingDatasetBuilder createTrainingDataset() {
    return TrainingDataset.buildTrainingDataset()
        .featureStore(this);
  }

  @Deprecated
  public TrainingDataset getTrainingDataset(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return (TrainingDataset) trainingDatasetApi.getTrainingDataset(this, name, version);
  }

  @Deprecated
  public TrainingDataset getTrainingDataset(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting training dataset `" + name + "`, defaulting to `"
        + FeatureStoreBase.DEFAULT_VERSION + "`.");
    return getTrainingDataset(name, FeatureStoreBase.DEFAULT_VERSION);
  }

  @Deprecated
  public scala.collection.Seq<TrainingDataset> getTrainingDatasets(@NonNull String name)
      throws FeatureStoreException, IOException {

    List<TrainingDataset> trainingDatasets = new ArrayList<>();
    for (TrainingDatasetBase td: trainingDatasetApi.get(this, name, null)) {
      trainingDatasets.add((TrainingDataset) td);
    }
    return JavaConverters.asScalaBufferConverter(trainingDatasets).asScala().toSeq();
  }
}
