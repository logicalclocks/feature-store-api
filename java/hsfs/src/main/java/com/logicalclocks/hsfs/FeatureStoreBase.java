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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class FeatureStoreBase<T2 extends QueryBase> {

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

  protected FeatureGroupApi featureGroupApi;
  protected TrainingDatasetApi trainingDatasetApi;
  protected StorageConnectorApi storageConnectorApi;

  protected static final Logger LOGGER = LoggerFactory.getLogger(FeatureStoreBase.class);

  protected static final Integer DEFAULT_VERSION = 1;

  public abstract Object createFeatureGroup();

  public abstract Object getFeatureGroups(@NonNull String name) throws FeatureStoreException, IOException;

  public abstract Object getOrCreateFeatureGroup(String name, Integer version)
      throws IOException, FeatureStoreException;

  public abstract Object getOrCreateFeatureGroup(String name, Integer version,
                                                 List<String> primaryKeys,
                                                 List<String> partitionKeys,
                                                 boolean onlineEnabled,
                                                 String eventTime) throws IOException, FeatureStoreException;

  public abstract Object getOrCreateFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                 boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException;

  public abstract Object getOrCreateFeatureGroup(String name, Integer version, String description,
                                                 List<String> primaryKeys, List<String> partitionKeys,
                                                 String hudiPrecombineKey, boolean onlineEnabled,
                                                 TimeTravelFormat timeTravelFormat, StatisticsConfig statisticsConfig,
                                                 String topicName, String notificationTopicName, String eventTime)
      throws IOException, FeatureStoreException;

  /**
   * Get a feature group object from the feature store.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract Object getStreamFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException;

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract Object getStreamFeatureGroup(String name) throws FeatureStoreException, IOException;

  public abstract Object createStreamFeatureGroup();

  public abstract Object getOrCreateStreamFeatureGroup(String name, Integer version) throws IOException,
      FeatureStoreException;

  public abstract Object getOrCreateStreamFeatureGroup(String name, Integer version,
                                                                   List<String> primaryKeys, boolean onlineEnabled,
                                                                   String eventTime) throws IOException,
      FeatureStoreException;

  public abstract Object getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                       List<String> partitionKeys, boolean onlineEnabled,
                                                       String eventTime)
      throws IOException, FeatureStoreException;

  public abstract Object getOrCreateStreamFeatureGroup(String name, Integer version, String description,
                                                List<String> primaryKeys, List<String> partitionKeys,
                                                String hudiPrecombineKey, boolean onlineEnabled,
                                                StatisticsConfig statisticsConfig, String eventTime)
      throws IOException, FeatureStoreException;

  public abstract Object  createExternalFeatureGroup();

  public abstract Object createFeatureView();

  public abstract Object getFeatureView(String name) throws FeatureStoreException, IOException;

  public abstract Object getFeatureView(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException;

  public abstract Object getOrCreateFeatureView(String name, T2 query, Integer version)
      throws FeatureStoreException, IOException;

  public abstract Object getOrCreateFeatureView(String name, T2 query, Integer version, String description,
                                                List<String> labels) throws FeatureStoreException, IOException;

  /**
   * Get a external feature group object from the feature store.
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return ExternalFeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract Object getExternalFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException;

  /**
   * Get a external feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return ExternalFeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract Object getExternalFeatureGroup(String name) throws FeatureStoreException, IOException;
  /**
   * Get a list of all versions of an external feature group from the feature store.
   *
   * @param name    the name of the feature group
   * @return ExternalFeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */

  public abstract StorageConnector getStorageConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getHopsFsConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getExternalFeatureGroups(@NonNull String name) throws FeatureStoreException, IOException;

  public abstract Object sql(String query);

  public abstract Object getJdbcConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getS3Connector(String name) throws FeatureStoreException, IOException;

  public abstract Object getRedshiftConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getSnowflakeConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getAdlsConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getKafkaConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getBigqueryConnector(String name) throws FeatureStoreException, IOException;

  public abstract Object getOnlineStorageConnector() throws FeatureStoreException, IOException;

  public abstract Object getGcsConnector(String name) throws FeatureStoreException, IOException;

  /**
   * Get a training dataset object from the selected feature store.
   *
   * @param name    name of the training dataset
   * @param version version to get
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract TrainingDatasetBase getTrainingDataset(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException;

  /**
   * Get a training dataset object with the default version `1` from the selected feature store.
   *
   * @param name name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract TrainingDatasetBase getTrainingDataset(String name) throws FeatureStoreException, IOException;

  /**
   * Get all versions of a training dataset object from the selected feature store.
   *
   * @param name    name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract Object getTrainingDatasets(@NonNull String name)
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
