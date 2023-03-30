/*
 *  Copyright (c) 2023. Hopsworks AB
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

package com.logicalclocks.hsfs.flink;

import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.TrainingDatasetBase;
import com.logicalclocks.hsfs.flink.constructor.Query;
import com.logicalclocks.hsfs.flink.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;

import com.logicalclocks.hsfs.flink.engine.FeatureGroupEngine;

import lombok.NonNull;

import java.io.IOException;
import java.util.List;

public class FeatureStore extends FeatureStoreBase<Query> {

  private FeatureGroupEngine featureGroupEngine;
  private FeatureViewEngine featureViewEngine;

  public FeatureStore() {
    storageConnectorApi = new StorageConnectorApi();
    featureViewEngine = new FeatureViewEngine();
    featureGroupEngine = new FeatureGroupEngine();
  }

  @Override
  public Object createFeatureGroup() {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getFeatureGroups(@NonNull String s) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getOrCreateFeatureGroup(String s, Integer integer) throws IOException, FeatureStoreException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getOrCreateFeatureGroup(String s, Integer integer, List<String> list, List<String> list1, boolean b,
      String s1) throws IOException, FeatureStoreException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getOrCreateFeatureGroup(String s, Integer integer, List<String> list, boolean b, String s1)
      throws IOException, FeatureStoreException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getOrCreateFeatureGroup(String s, Integer integer, String s1, List<String> list, List<String> list1,
      String s2, boolean b, TimeTravelFormat timeTravelFormat, StatisticsConfig statisticsConfig, String s3)
      throws IOException, FeatureStoreException {
    throw new UnsupportedOperationException("Not supported for Flink");
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
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupEngine.getStreamFeatureGroup(this, name, version);
  }

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getStreamFeatureGroup(name, DEFAULT_VERSION);
  }

  @Override
  public StreamFeatureGroup.StreamFeatureGroupBuilder createStreamFeatureGroup() {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version)
      throws IOException, FeatureStoreException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          List<String> partitionKeys, boolean onlineEnabled,
                                                          String eventTime) throws IOException, FeatureStoreException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, String description,
                                                          List<String> primaryKeys, List<String> partitionKeys,
                                                          String hudiPrecombineKey, boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig,
                                                          String eventTime)
      throws IOException, FeatureStoreException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object createExternalFeatureGroup() {
    return null;
  }

  @Override
  public Object createFeatureView() {
    return null;
  }

  @Override
  public StorageConnector getStorageConnector(String s) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getHopsFsConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getJdbcConnector(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getOnlineStorageConnector() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getGcsConnector(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getS3Connector(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getRedshiftConnector(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getSnowflakeConnector(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getAdlsConnector(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getKafkaConnector(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getBigqueryConnector(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getExternalFeatureGroups(@NonNull String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object sql(String query) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public TrainingDatasetBase getTrainingDataset(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public TrainingDatasetBase getTrainingDataset(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getTrainingDatasets(@NonNull String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public FeatureView getOrCreateFeatureView(String name, Query query, Integer version)
      throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public FeatureView getOrCreateFeatureView(String name, Query query, Integer version, String description,
                                            List<String> labels) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
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

  @Override
  public Object getExternalFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getExternalFeatureGroup(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }
}
