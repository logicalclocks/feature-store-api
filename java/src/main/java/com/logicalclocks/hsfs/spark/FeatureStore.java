/*
 *  Copyright (c) 2022. Logical Clocks AB
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

import com.logicalclocks.hsfs.generic.FeatureStoreException;
import com.logicalclocks.hsfs.generic.StatisticsConfig;
import com.logicalclocks.hsfs.generic.StreamFeatureGroup;
import com.logicalclocks.hsfs.generic.TimeTravelFormat;
import com.logicalclocks.hsfs.generic.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.generic.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.generic.metadata.TrainingDatasetApi;
import com.logicalclocks.hsfs.spark.TrainingDataset;
import com.logicalclocks.hsfs.spark.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FeatureStore extends com.logicalclocks.hsfs.generic.FeatureStore {

  private FeatureGroupApi featureGroupApi;
  private FeatureGroupEngine featureGroupEngine;
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

  @Override
  public Object createStreamFeatureGroup() {
    return null;
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version) {
    return null;
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          boolean onlineEnabled, String eventTime) {
    return null;
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          List<String> partitionKeys, boolean onlineEnabled,
                                                          String eventTime) {
    return null;
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, String description,
                                                          List<String> primaryKeys, List<String> partitionKeys,
                                                          String hudiPrecombineKey, boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig, String eventTime) {
    return null;
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
  public FeatureGroup getFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return (FeatureGroup) featureGroupApi.getFeatureGroup(this, name, version);
  }

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public FeatureGroup getFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getFeatureGroup(name, DEFAULT_VERSION);
  }

  /**
   * Get a list of all versions of a feature group from the feature store.
   *
   * @param name    the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public scala.collection.Seq<FeatureGroup> getFeatureGroups(@NonNull String name)
      throws FeatureStoreException, IOException {
    List<FeatureGroup> featureGroups = new ArrayList<>();
    for (FeatureGroupBase fg: featureGroupApi.getFeatureGroups(this, name)) {
      featureGroups.add((FeatureGroup) fg);
    }
    return JavaConverters.asScalaBufferConverter(featureGroups)
        .asScala().toSeq();
  }

  public FeatureGroup.FeatureGroupBuilder createFeatureGroup() {
    return FeatureGroup.builder()
        .featureStore(this);
  }

  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version) throws IOException, FeatureStoreException {
    return   featureGroupEngine.getOrCreateFeatureGroup(this, name, version, null, null,
        null, null, false, null, null, null);
  }

  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                              boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException {
    return   featureGroupEngine.getOrCreateFeatureGroup(this, name, version, null, primaryKeys,
        null, null, onlineEnabled, null, null, eventTime);
  }

  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version,
                                              List<String> primaryKeys,
                                              List<String> partitionKeys,
                                              boolean onlineEnabled,
                                              String eventTime) throws IOException, FeatureStoreException {

    return   featureGroupEngine.getOrCreateFeatureGroup(this, name, version, null, primaryKeys,
        partitionKeys, null, onlineEnabled, null, null, eventTime);
  }

  public FeatureGroup getOrCreateFeatureGroup(String name, Integer version, String description,
                                              List<String> primaryKeys, List<String> partitionKeys,
                                              String hudiPrecombineKey,
                                              boolean onlineEnabled, TimeTravelFormat timeTravelFormat,
                                              StatisticsConfig statisticsConfig, String eventTime)
      throws IOException, FeatureStoreException {

    return   featureGroupEngine.getOrCreateFeatureGroup(this, name, version, description, primaryKeys,
        partitionKeys, hudiPrecombineKey, onlineEnabled, timeTravelFormat, statisticsConfig, eventTime);
  }

  @Override
  public Dataset<Row> sql(String query) {
    return SparkEngine.getInstance().sql(query);
  }

  @Override
  public StorageConnector.JdbcConnector getJdbcConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.JdbcConnector) storageConnectorApi.getByName(this, name);
  }

  @Override
  public StorageConnector.S3Connector getS3Connector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.S3Connector) storageConnectorApi.getByName(this, name);
  }

  @Override
  public StorageConnector.RedshiftConnector getRedshiftConnector(String name)
      throws FeatureStoreException, IOException {
    return (StorageConnector.RedshiftConnector) storageConnectorApi.getByName(this, name);
  }

  @Override
  public StorageConnector.SnowflakeConnector getSnowflakeConnector(String name)
      throws FeatureStoreException, IOException {
    return (StorageConnector.SnowflakeConnector) storageConnectorApi.getByName(this, name);
  }

  @Override
  public StorageConnector.AdlsConnector getAdlsConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.AdlsConnector) storageConnectorApi.getByName(this, name);
  }

  @Override
  public StorageConnector.KafkaConnector getKafkaConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.KafkaConnector) storageConnectorApi.getByName(this, name);
  }

  @Override
  public StorageConnector.BigqueryConnector getBigqueryConnector(String name) throws FeatureStoreException,
      IOException {
    return (StorageConnector.BigqueryConnector) storageConnectorApi.getByName(this, name);
  }

  @Override
  public StorageConnector.JdbcConnector getOnlineStorageConnector() throws FeatureStoreException, IOException {
    return storageConnectorApi.getOnlineStorageConnector(this);
  }

  @Override
  public StorageConnector.GcsConnector getGcsConnector(String name) throws FeatureStoreException, IOException {
    return (StorageConnector.GcsConnector) storageConnectorApi.getByName(this, name);
  }

  public TrainingDataset.TrainingDatasetBuilder createTrainingDataset() {
    return TrainingDataset.builder()
        .featureStore(this);
  }

  /**
   * Get a training dataset object from the selected feature store.
   *
   * @param name    name of the training dataset
   * @param version version to get
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public TrainingDataset getTrainingDataset(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return (TrainingDataset) trainingDatasetApi.getTrainingDataset(this, name, version);
  }

  /**
   * Get a training dataset object with the default version `1` from the selected feature store.
   *
   * @param name name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public TrainingDataset getTrainingDataset(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting training dataset `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getTrainingDataset(name, DEFAULT_VERSION);
  }

  @Override
  public Seq<TrainingDataset> getTrainingDatasets(@NonNull String name)
      throws FeatureStoreException, IOException {

    List<TrainingDataset> trainingDatasets = new ArrayList<>();
    for (com.logicalclocks.hsfs.generic.TrainingDataset td: trainingDatasetApi.get(this, name, null)) {
      trainingDatasets.add((TrainingDataset) td);
    }
    return JavaConverters.asScalaBufferConverter(trainingDatasets).asScala().toSeq();
  }
}
