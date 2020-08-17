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
import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FeatureStore {

  @Getter @Setter
  @JsonProperty("featurestoreId")
  private Integer id;

  @Getter @Setter
  @JsonProperty("featurestoreName")
  private String name;

  @Getter @Setter
  private Integer projectId;

  private FeatureGroupApi featureGroupApi;
  private TrainingDatasetApi trainingDatasetApi;
  private StorageConnectorApi storageConnectorApi;

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureStore.class);

  private static final Integer DEFAULT_VERSION = 1;

  public FeatureStore() throws FeatureStoreException {
    featureGroupApi = new FeatureGroupApi();
    trainingDatasetApi = new TrainingDatasetApi();
    storageConnectorApi = new StorageConnectorApi();
  }

  /**
   * Get a feature group object from the feature store.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public FeatureGroup getFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupApi.get(this, name, version);
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

  public Dataset<Row> sql(String query) {
    return SparkEngine.getInstance().sql(query);
  }

  public StorageConnector getStorageConnector(String name, StorageConnectorType type)
      throws FeatureStoreException, IOException {
    return storageConnectorApi.getByNameAndType(this, name, type);
  }

  public FeatureGroup.FeatureGroupBuilder createFeatureGroup() {
    return FeatureGroup.builder()
        .featureStore(this);
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
   * @throws FeatureStoreException
   * @throws IOException
   */
  public TrainingDataset getTrainingDataset(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return trainingDatasetApi.get(this, name, version);
  }

  /**
   * Get a training dataset object with the default version `1` from the selected feature store.
   *
   * @param name name of the training dataset
   * @return TrainingDataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public TrainingDataset getTrainingDataset(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting training dataset `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getTrainingDataset(name, DEFAULT_VERSION);
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
