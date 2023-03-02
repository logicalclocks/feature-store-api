/*
 *  Copyright (c) 2020-2022. Hopsworks AB
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

package com.logicalclocks.base;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.base.engine.FeatureViewEngineBase;
import com.logicalclocks.base.metadata.FeatureGroupApi;
import com.logicalclocks.base.metadata.StorageConnectorApi;
import com.logicalclocks.base.metadata.TrainingDatasetApi;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class FeatureStoreBase {

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
  protected FeatureViewEngineBase featureViewEngineBase;

  protected static final Integer DEFAULT_VERSION = 1;

  /**
   * Get a feature group object from the feature store.
   *
   * @param name    the name of the feature group
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

  public abstract Object  createExternalFeatureGroup();

  public abstract FeatureViewBase getFeatureView(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException;

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

  public abstract StorageConnectorBase getStorageConnector(String name) throws FeatureStoreException, IOException;

  public abstract StorageConnectorBase.HopsFsConnectorBase getHopsFsConnector(String name)
      throws FeatureStoreException, IOException;

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
