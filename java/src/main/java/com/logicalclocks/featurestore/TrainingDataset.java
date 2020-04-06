package com.logicalclocks.featurestore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.logicalclocks.featurestore.engine.TrainingDatasetEngine;
import com.logicalclocks.featurestore.metadata.Query;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class TrainingDataset {
  @Getter @Setter
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private Integer version;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private DataFormat dataFormat;

  @Getter @Setter
  private TrainingDatasetType trainingDatasetType = TrainingDatasetType.HOPSFS_TRAINING_DATASET;

  @Getter @Setter
  private List<Feature> features;

  @Getter @Setter
  @JsonIgnore
  private FeatureStore featureStore;

  @Getter @Setter
  private Integer storageConnectorId;

  @Getter @Setter
  private String location;

  @Getter @Setter
  @JsonIgnore
  private Map<String, Double> splits;

  private TrainingDatasetEngine trainingDatasetEngine = new TrainingDatasetEngine();

  @Builder
  public TrainingDataset(@NonNull String name, @NonNull Integer version, String description,
                         DataFormat dataFormat, StorageConnector storageConnector,
                         String location, Map<String, Double> splits,
                         FeatureStore featureStore) {
    this.name = name;
    this.version = version;
    this.description = description;
    this.dataFormat = dataFormat;
    this.location = location;

    if (storageConnector != null) {
      this.storageConnectorId = storageConnector.getId();
      if (storageConnector.getStorageConnectorType() == StorageConnectorType.S3) {
        // Default it's already HOPSFS_TRAINING_DATASET
        this.trainingDatasetType = TrainingDatasetType.EXTERNAL_TRAINING_DATASET;
      }
    }

    this.splits = splits;
    this.featureStore = featureStore;
  }

  /**
   * Create the training dataset based on the content of the feature store query
   * @param query
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void create(Query query) throws FeatureStoreException, IOException {
    create(query, null);
  }

  /**
   * Create the training dataset based on teh content of the dataset
   * @param dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void create(Dataset<Row> dataset) throws FeatureStoreException, IOException {
    create(dataset, null);
  }

  /**
   * Create the training dataset based on the content of the feature store query
   * @param query
   * @param writeOptions: options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void create(Query query, Map<String, String> writeOptions) throws FeatureStoreException, IOException {
    trainingDatasetEngine.create(this, query.read(), writeOptions);
  }

  /**
   * Create the training dataset based on teh content of the dataset
   * @param dataset
   * @param writeOptions: options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void create(Dataset<Row> dataset, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    trainingDatasetEngine.create(this, dataset, writeOptions);
  }

  /**
   * Insert the content of the feature store query in the training dataset
   * @param query
   * @param overwrite: true to overwrite the current content of the training dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Query query, boolean overwrite) throws FeatureStoreException, IOException {
    insert(query, overwrite, new HashMap<>());
  }

  /**
   * Insert the content of the dataset in the training dataset
   * @param dataset
   * @param overwrite: true to overwrite the current content of the training dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Dataset<Row> dataset, boolean overwrite) throws FeatureStoreException, IOException {
    insert(dataset, overwrite, new HashMap<>());
  }

  /**
   * Insert the content of the feature store query in the training dataset
   * @param query
   * @param overwrite: true to overwrite the current content of the training dataset
   * @param writeOptions: options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Query query, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    trainingDatasetEngine.insert(this, query.read(),
        writeOptions, overwrite ? SaveMode.Overwrite : SaveMode.Append);
  }

  /**
   * Insert the content of the dataset in the training dataset
   * @param dataset
   * @param overwrite: true to overwrite the current content of the training dataset
   * @param writeOptions: options to pass to the Spark write operation
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Dataset<Row> dataset, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    trainingDatasetEngine.insert(this, dataset,
        writeOptions, overwrite ? SaveMode.Overwrite : SaveMode.Append);
  }

  /**
   * Read the content (all splits if multiple available) of the training dataset
   * @return
   */
  public Dataset<Row> read() {
    return read(new HashMap<>());
  }

  /**
   * Read the content (all splits if multiple available) of the training dataset
   * @param readOptions: options to pass to the Spark read operation
   * @return
   */
  public Dataset<Row> read(Map<String, String> readOptions) {
    return trainingDatasetEngine.read(this, "", readOptions);
  }

  /**
   * Read all a single split from the training dataset
   * @param split: the split name
   * @return
   */
  public Dataset<Row> read(String split) {
    return read(split, new HashMap<>());
  }


  /**
   * Read a single split from the training dataset
   * @param split: the split name
   * @param readOptions: options to pass to the Spark read operation
   * @return
   */
  public Dataset<Row> read(String split, Map<String, String> readOptions) {
    return trainingDatasetEngine.read(this, split, readOptions);
  }

  /**
   * Show numRows from the training dataset (across all splits)
   * @param numRows
   */
  public void show(int numRows) {
    read(new HashMap<>()).show(numRows);
  }
}
