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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.List;

public class FeatureStore {

  @Getter
  @Setter
  @JsonProperty("featurestoreId")
  private Integer id;

  @Getter
  @Setter
  @JsonProperty("featurestoreName")
  private String name;

  @Getter
  @Setter
  private Integer projectId;

  private FeatureGroupApi featureGroupApi;
  private TrainingDatasetApi trainingDatasetApi;
  private StorageConnectorApi storageConnectorApi;
  private FeatureViewEngine featureViewEngine;

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureStore.class);

  private static final Integer DEFAULT_VERSION = 1;

  public FeatureStore() {
    featureGroupApi = new FeatureGroupApi();
    trainingDatasetApi = new TrainingDatasetApi();
    storageConnectorApi = new StorageConnectorApi();
    featureViewEngine = new FeatureViewEngine();
  }

  /**
   * Get a feature group object from the feature store.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public FeatureGroup getFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupApi.getFeatureGroup(this, name, version);
  }

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public FeatureGroup getFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getFeatureGroup(name, DEFAULT_VERSION);
  }

  /**
   * Get a list of all versions of a feature group from the feature store.
   *
   * @param name the name of the feature group
   * @return list of FeatureGroups
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public scala.collection.Seq<FeatureGroup> getFeatureGroups(@NonNull String name)
      throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(featureGroupApi.getFeatureGroups(this, name))
        .asScala().toSeq();
  }

  /**
   * Get a feature group object from the feature store.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public StreamFeatureGroup getStreamFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupApi.getStreamFeatureGroup(this, name, version);
  }

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public StreamFeatureGroup getStreamFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getStreamFeatureGroup(name, DEFAULT_VERSION);
  }

  /**
   * Get a external feature group object from the feature store.
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return ExternalFeatureGroup
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public ExternalFeatureGroup getExternalFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupApi.getExternalFeatureGroup(this, name, version);
  }

  /**
   * Get a external feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return ExternalFeatureGroup
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public ExternalFeatureGroup getExternalFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getExternalFeatureGroup(name, DEFAULT_VERSION);
  }

  /**
   * Get a list of all versions of an external feature group from the feature store.
   *
   * @param name    the name of the feature group
   * @return list of ExternalFeatureGroup objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public scala.collection.Seq<ExternalFeatureGroup> getExternalFeatureGroups(@NonNull String name)
      throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(featureGroupApi.getExternalFeatureGroups(this, name))
        .asScala().toSeq();
  }

  @Deprecated
  public ExternalFeatureGroup getOnDemandFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupApi.getExternalFeatureGroup(this, name, version);
  }

  @Deprecated
  public ExternalFeatureGroup getOnDemandFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getExternalFeatureGroup(name, DEFAULT_VERSION);
  }

  @Deprecated
  public scala.collection.Seq<ExternalFeatureGroup> getOnDemandFeatureGroups(@NonNull String name)
      throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(featureGroupApi.getExternalFeatureGroups(this, name))
        .asScala().toSeq();
  }

  public Object sql(String query) {
    return SparkEngine.getInstance().sql(query);
  }

  public StorageConnector getStorageConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.JdbcConnector getJdbcConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.JdbcConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.S3Connector getS3Connector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.S3Connector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.HopsFsConnector getHopsFsConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.HopsFsConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.RedshiftConnector getRedshiftConnector(String name)
      throws FeatureStoreException, IOException {
    return (StorageConnector.RedshiftConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.SnowflakeConnector getSnowflakeConnector(String name)
      throws FeatureStoreException, IOException {
    return (StorageConnector.SnowflakeConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.AdlsConnector getAdlsConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.AdlsConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.KafkaConnector getKafkaConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.KafkaConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.BigqueryConnector getBigqueryConnector(String name) throws FeatureStoreException,
      IOException {
    return (StorageConnector.BigqueryConnector) storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.JdbcConnector getOnlineStorageConnector() throws FeatureStoreException, IOException {
    return storageConnectorApi.getOnlineStorageConnector(this);
  }

  public StorageConnector.GcsConnector getGcsConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.GcsConnector) storageConnectorApi.getByName(this, name);
  }

  public FeatureGroup.FeatureGroupBuilder createFeatureGroup() {
    return FeatureGroup.builder()
        .featureStore(this);
  }

  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version) throws IOException, FeatureStoreException {
    return featureGroupApi.getOrCreateFeatureGroup(this, name, version, null, null,
        null, null, false, null, null, null);
  }

  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                              boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException {
    return featureGroupApi.getOrCreateFeatureGroup(this, name, version, null, primaryKeys,
        null, null, onlineEnabled, null, null, eventTime);
  }

  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version,
                                              List<String> primaryKeys,
                                              List<String> partitionKeys,
                                              boolean onlineEnabled,
                                              String eventTime) throws IOException, FeatureStoreException {

    return featureGroupApi.getOrCreateFeatureGroup(this, name, version, null, primaryKeys,
        partitionKeys, null, onlineEnabled, null, null, eventTime);
  }

  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version, String description,
                                              List<String> primaryKeys, List<String> partitionKeys,
                                              String hudiPrecombineKey,
                                              boolean onlineEnabled, TimeTravelFormat timeTravelFormat,
                                              StatisticsConfig statisticsConfig, String eventTime)
      throws IOException, FeatureStoreException {

    return featureGroupApi.getOrCreateFeatureGroup(this, name, version, description, primaryKeys,
        partitionKeys, hudiPrecombineKey, onlineEnabled, timeTravelFormat, statisticsConfig, eventTime);
  }

  public StreamFeatureGroup.StreamFeatureGroupBuilder createStreamFeatureGroup() {
    return StreamFeatureGroup.builder()
        .featureStore(this);
  }

  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version)
      throws IOException, FeatureStoreException {
    return featureGroupApi.getOrCreateStreamFeatureGroup(this, name, version, null,
        null, null, null, false, null, null);
  }

  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException {
    return featureGroupApi.getOrCreateStreamFeatureGroup(this, name, version, null,
        primaryKeys, null, null, onlineEnabled, null, eventTime);
  }

  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          List<String> partitionKeys, boolean onlineEnabled,
                                                          String eventTime) throws IOException, FeatureStoreException {

    return featureGroupApi.getOrCreateStreamFeatureGroup(this, name, version, null,
        primaryKeys, partitionKeys, null, onlineEnabled, null, eventTime);
  }

  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, String description,
                                                          List<String> primaryKeys, List<String> partitionKeys,
                                                          String hudiPrecombineKey, boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig,
                                                          String eventTime)
      throws IOException, FeatureStoreException {

    return featureGroupApi.getOrCreateStreamFeatureGroup(this, name, version, description,
        primaryKeys, partitionKeys, hudiPrecombineKey, onlineEnabled, statisticsConfig, eventTime);
  }

  public ExternalFeatureGroup.ExternalFeatureGroupBuilder createExternalFeatureGroup() {
    return ExternalFeatureGroup.builder()
        .featureStore(this);
  }

  @Deprecated
  public ExternalFeatureGroup.ExternalFeatureGroupBuilder createOnDemandFeatureGroup() {
    return ExternalFeatureGroup.builder()
        .featureStore(this);
  }

  public FeatureView.FeatureViewBuilder createFeatureView() {
    return new FeatureView.FeatureViewBuilder(this);
  }

  /**
   * Get feature view metadata object or create a new one if it doesn't exist. This method doesn't update
   * existing feature view metadata object.
   *
   * @param name name of the feature view
   * @param query Query object
   * @param version version of the feature view
   * @return FeatureView
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public FeatureView getOrCreateFeatureView(String name, Query query, Integer version)
      throws FeatureStoreException, IOException {
    return featureViewEngine.getOrCreateFeatureView(this, name, version, query, null, null);
  }

  /**
   * Get feature view metadata object or create a new one if it doesn't exist. This method doesn't update
   * existing feature view metadata object.
   *
   * @param name name of the feature view
   * @param query Query object
   * @param version version of the feature view
   * @param description description of the feature view
   * @param labels list of label features
   * @return FeatureView
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public FeatureView getOrCreateFeatureView(String name, Query query, Integer version, String description,
                                            List<String> labels) throws FeatureStoreException, IOException {
    return featureViewEngine.getOrCreateFeatureView(this, name, version, query, description, labels);
  }

  /**
   * Get a feature view object from the selected feature store.
   *
   * @param name    name of the feature view
   * @param version version to get
   * @return FeatureView
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public FeatureView getFeatureView(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureViewEngine.get(this, name, version);
  }

  /**
   * Get a feature view object with the default version `1` from the selected feature store.
   *
   * @param name name of the feature view
   * @return FeatureView
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public FeatureView getFeatureView(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature view `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getFeatureView(name, DEFAULT_VERSION);
  }

  public TrainingDataset.TrainingDatasetBuilder createTrainingDataset() {
    return TrainingDataset.builder()
        .featureStore(this);
  }

  /**
   * Get a training dataset object from the selected feature store.
   *
   * @param name name of the training dataset
   * @param version version to get
   * @return TrainingDataset
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public TrainingDataset getTrainingDataset(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return trainingDatasetApi.getTrainingDataset(this, name, version);
  }

  /**
   * Get a training dataset object with the default version `1` from the selected feature store.
   *
   * @param name name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public TrainingDataset getTrainingDataset(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting training dataset `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getTrainingDataset(name, DEFAULT_VERSION);
  }

  /**
   * Get all versions of a training dataset object from the selected feature store.
   *
   * @param name name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public scala.collection.Seq<TrainingDataset> getTrainingDatasets(@NonNull String name)
      throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(trainingDatasetApi.get(this, name, null)).asScala().toSeq();
  }

  @Override
  public String toString() {
    return "FeatureStore{"
        + "id=" + id
        + ", name='" + name + '\''
        + ", projectId=" + projectId
        + ", featureGroupApi=" + featureGroupApi
        + '}';
  }
}
