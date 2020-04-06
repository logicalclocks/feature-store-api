package com.logicalclocks.featurestore;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.logicalclocks.featurestore.engine.SparkEngine;
import com.logicalclocks.featurestore.metadata.FeatureGroupApi;
import com.logicalclocks.featurestore.metadata.TrainingDatasetApi;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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

  public FeatureStore() throws FeatureStoreException {
    featureGroupApi = new FeatureGroupApi();
    trainingDatasetApi = new TrainingDatasetApi();
    storageConnectorApi = new StorageConnectorApi();
  }

  /**
   * Get a feature group from the feature store
   * @param name: the name of the feature group
   * @param version: the version of the feature group
   * @return
   * @throws FeatureStoreException
   */
  public FeatureGroup getFeatureGroup(String name, Integer version)
      throws FeatureStoreException, IOException {
    if (Strings.isNullOrEmpty(name) || version == null) {
      throw new FeatureStoreException("Both name and version are required");
    }
    return featureGroupApi.get(this, name, version);
  }

  public Dataset<Row> sql(String query) {
    return SparkEngine.getInstance().sql(query);
  }

  public StorageConnector getStorageConnector(String name, StorageConnectorType type)
     throws FeatureStoreException, IOException {
    return storageConnectorApi.getByNameAndType(this, name, type);
  }

  public TrainingDataset.TrainingDatasetBuilder createTrainingDataset() {
    return TrainingDataset.builder()
        .featureStore(this);
  }

  /**
   * Get a training dataset object from the selected feature store
   * @param name: name of the training dataset
   * @param version: version to get
   * @return
   * @throws FeatureStoreException
   * @throws IOException
   */
  public TrainingDataset getTrainingDataset(String name, Integer version)
      throws FeatureStoreException, IOException {
    if (Strings.isNullOrEmpty(name) || version == null) {
      throw new FeatureStoreException("Both name and version are required");
    }
    return trainingDatasetApi.get(this, name, version);
  }

  @Override
  public String toString() {
    return "FeatureStore{" +
        "id=" + id +
        ", name='" + name + '\'' +
        ", projectId=" + projectId +
        ", featureGroupApi=" + featureGroupApi +
        '}';
  }
}
