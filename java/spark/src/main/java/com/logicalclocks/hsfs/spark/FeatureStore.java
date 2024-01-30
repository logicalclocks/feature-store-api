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
  @Override
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
  @Override
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
  @Override
  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version) throws IOException, FeatureStoreException {
    return   featureGroupEngine.getOrCreateFeatureGroup(this, name, version, null, null,
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
  @Override
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
  @Override
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
  @Override
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

  /**
   * Create stream feature group builder object.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StreamFeatureGroup.StreamFeatureGroupBuilder = fs.createStreamFeatureGroup()
   * }
   * </pre>
   *
   * @return StreamFeatureGroup.StreamFeatureGroupBuilder a StreamFeatureGroup builder object.
   */
  public StreamFeatureGroup.StreamFeatureGroupBuilder createStreamFeatureGroup() {
    return StreamFeatureGroup.builder()
        .featureStore(this);
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
  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version)
      throws IOException, FeatureStoreException {
    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, null,
        null, null, null, false, null, null);
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
  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException {
    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, null,
        primaryKeys, null, null, onlineEnabled, null, eventTime);
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
  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          List<String> partitionKeys, boolean onlineEnabled,
                                                          String eventTime) throws IOException, FeatureStoreException {


    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, null,
        primaryKeys, partitionKeys, null, onlineEnabled, null, eventTime);
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
  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, String description,
                                                          List<String> primaryKeys, List<String> partitionKeys,
                                                          String hudiPrecombineKey, boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig,
                                                          String eventTime)
      throws IOException, FeatureStoreException {

    return featureGroupEngine.getOrCreateStreamFeatureGroup(this, name, version, description,
        primaryKeys, partitionKeys, hudiPrecombineKey, onlineEnabled, statisticsConfig, eventTime);
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
  @Override
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
  @Override
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
  @Override
  public ExternalFeatureGroup getExternalFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + FeatureStoreBase.DEFAULT_VERSION + "`.");
    return getExternalFeatureGroup(name, FeatureStoreBase.DEFAULT_VERSION);
  }

  /**
   * Get a previously created storage connector from the feature store.
   *
   * <p>Storage connectors encapsulate all information needed for the execution engine to read and write to a specific
   * storage.
   *
   * <p>If you want to connect to the online feature store, see the getOnlineStorageConnector` method to get the
   * JDBC connector for the Online Feature Store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector sc = fs.getStorageConnector("sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector getStorageConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.class);
  }

  /**
   * Get a previously created HopsFs compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.HopsFsConnector hfsSc = fs.getHopsFsConnector("hfs_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.HopsFsConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.HopsFsConnector getHopsFsConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.HopsFsConnector.class);
  }

  /**
   * Get a previously created JDBC compliant storage connector from the feature store.
   *
   * <p>If you want to connect to the online feature store, see the getOnlineStorageConnector` method to get the
   * JDBC connector for the Online Feature Store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.JdbcConnector jdbcSc = fs.getJdbcConnector("jdbc_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the jdbc storage connector to retrieve.
   * @return StorageConnector.JdbcConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.JdbcConnector getJdbcConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.JdbcConnector.class);
  }

  /**
   * Get a previously created JDBC compliant storage connector from the feature store
   * to connect to the online feature store.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.JdbcConnector onlineSc = fs.getOnlineStorageConnector("online_sc_name");
   * }
   * </pre>
   *
   * @return StorageConnector.JdbcConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.JdbcConnector getOnlineStorageConnector() throws FeatureStoreException, IOException {
    return storageConnectorApi.getOnlineStorageConnector(this, StorageConnector.JdbcConnector.class);
  }

  /**
   * Get a previously created S3 compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.S3Connector s3Sc = fs.getS3Connector("s3_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.S3Connector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.S3Connector getS3Connector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.S3Connector.class);
  }

  /**
   * Get a previously created Redshift compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.RedshiftConnector rshSc = fs.getRedshiftConnector("rsh_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.RedshiftConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.RedshiftConnector getRedshiftConnector(String name)
      throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.RedshiftConnector.class);
  }

  /**
   * Get a previously created Snowflake compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.SnowflakeConnector snflSc = fs.getSnowflakeConnector("snfl_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.SnowflakeConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.SnowflakeConnector getSnowflakeConnector(String name)
      throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.SnowflakeConnector.class);
  }

  /**
   * Get a previously created Adls compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.AdlsConnectorr adlslSc = fs.getAdlsConnector("adls_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.AdlsConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.AdlsConnector getAdlsConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.AdlsConnector.class);
  }

  /**
   * Get a previously created Kafka compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.KafkaConnector kafkaSc = fs.getKafkaConnector("kafka_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.KafkaConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.KafkaConnector getKafkaConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.KafkaConnector.class);
  }

  /**
   * Get a previously created BigQuery compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.BigqueryConnector bigqSc = fs.getBigqueryConnector("bigq_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.BigqueryConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.BigqueryConnector getBigqueryConnector(String name) throws FeatureStoreException,
      IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.BigqueryConnector.class);
  }

  /**
   * Get a previously created Gcs compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.GcsConnector gcsSc = fs.getGcsConnector("gsc_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.GcsConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StorageConnector.GcsConnector getGcsConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.GcsConnector.class);
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

  @Override
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
  @Override
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
  @Override
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

  @Override
  public Dataset<Row> sql(String query) {
    return SparkEngine.getInstance().sql(query);
  }

  @Deprecated
  public TrainingDataset.TrainingDatasetBuilder createTrainingDataset() {
    return TrainingDataset.builder()
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
