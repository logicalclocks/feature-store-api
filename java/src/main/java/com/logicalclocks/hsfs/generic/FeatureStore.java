/*
 *  Copyright (c) 2020-2022. Logical Clocks AB
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

package com.logicalclocks.hsfs.generic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.hsfs.generic.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.generic.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.spark.ExternalFeatureGroup;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.List;

public abstract class FeatureStore {

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
  private StorageConnectorApi storageConnectorApi;
  private FeatureViewEngine featureViewEngine;

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureStore.class);

  private static final Integer DEFAULT_VERSION = 1;

  public FeatureStore() {
    featureGroupApi = new FeatureGroupApi();
    storageConnectorApi = new StorageConnectorApi();
    featureViewEngine = new FeatureViewEngine();
  }


  /**
   * Get a feature group object from the feature store.
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
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
   * @throws FeatureStoreException
   * @throws IOException
   */
  public StreamFeatureGroup getStreamFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getStreamFeatureGroup(name, DEFAULT_VERSION);
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
   * Get a feature view object from the selected feature store.
   *
   * @param name    name of the feature view
   * @param version version to get
   * @return FeatureView
   * @throws FeatureStoreException
   * @throws IOException
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
   * @throws FeatureStoreException
   * @throws IOException
   */
  public FeatureView getFeatureView(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature view `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getFeatureView(name, DEFAULT_VERSION);
  }

  /**
   * Get a external feature group object from the feature store.
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return ExternalFeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
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
   * @throws FeatureStoreException
   * @throws IOException
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
   * @return ExternalFeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
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

  public abstract Object sql(String query);

  public StorageConnector getStorageConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name);
  }

  public StorageConnector.HopsFsConnector getHopsFsConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.HopsFsConnector) storageConnectorApi.getByName(this, name);
  }

  public abstract Object getJdbcConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getS3Connector(String name) throws FeatureStoreException, IOException;

  public abstract Object getRedshiftConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getSnowflakeConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getAdlsConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getKafkaConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getBigqueryConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getOnlineStorageConnector() throws FeatureStoreException, IOException;

  public abstract Object getGcsConnector(String name) throws FeatureStoreException, IOException;

  public abstract TrainingDataset.TrainingDatasetBuilder createTrainingDataset();

  /**
   * Get a training dataset object from the selected feature store.
   *
   * @param name    name of the training dataset
   * @param version version to get
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract TrainingDataset getTrainingDataset(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException;

  /**
   * Get a training dataset object with the default version `1` from the selected feature store.
   *
   * @param name name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract TrainingDataset getTrainingDataset(String name) throws FeatureStoreException, IOException;

  /**
   * Get all versions of a training dataset object from the selected feature store.
   *
   * @param name    name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract Seq<com.logicalclocks.hsfs.spark.TrainingDataset> getTrainingDatasets(@NonNull String name)
      throws FeatureStoreException, IOException;

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
