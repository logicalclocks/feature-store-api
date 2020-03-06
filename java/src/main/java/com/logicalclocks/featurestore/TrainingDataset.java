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
  // TODO(Fabio): sort it out correctly
  private String trainingDatasetType = "HOPSFS_TRAINING_DATASET";

  @Getter @Setter
  private List<Feature> features;

  @Getter @Setter
  @JsonIgnore
  private Query featuresQuery;

  @Getter @Setter
  @JsonIgnore
  private Dataset<Row> featuresDataframe;

  @Getter @Setter
  @JsonIgnore
  private Map<String, String> writeOptions;

  @Getter @Setter
  @JsonIgnore
  private FeatureStore featureStore;

  @Getter @Setter
  private Integer storageConnectorId;

  @Getter @Setter
  private String location;

  @Getter @Setter
  @JsonIgnore
  private double[] splits;

  private TrainingDatasetEngine trainingDatasetEngine = new TrainingDatasetEngine();

  @Builder
  public TrainingDataset(Integer id, @NonNull String name, @NonNull Integer version, String description,
                         DataFormat dataFormat, Query featuresQuery, Dataset<Row> featuresDataframe,
                         Map<String, String> writeOptions, StorageConnector storageConnector, double[] splits,
                         FeatureStore featureStore)
      throws FeatureStoreException, IOException {
    if (featuresDataframe == null && featuresQuery == null) {
      throw new FeatureStoreException("Neither FeaturesDataFrame nor FeaturesQuery options was specified");
    } else if (featuresDataframe != null && featuresQuery != null) {
      throw new FeatureStoreException("Both FeaturesDataFrame and FeaturesQuery options were specified");
    }

    this.id = id;
    this.name = name;
    this.version = version;
    this.description = description;
    this.dataFormat = dataFormat;
    this.featuresQuery = featuresQuery;
    this.featuresDataframe = featuresDataframe;
    this.writeOptions = writeOptions;

    if (storageConnector != null) {
      this.storageConnectorId = storageConnector.getId();
    }

    this.splits = splits;
    this.featureStore = featureStore;

    trainingDatasetEngine.saveTrainingDataset(this);
  }

  public void insert(Query query, boolean overwrite) throws FeatureStoreException, IOException {
    insert(query, overwrite, new HashMap<>());
  }

  public void insert(Dataset<Row> dataset, boolean overwrite) throws FeatureStoreException, IOException {
    insert(dataset, overwrite, new HashMap<>());
  }

  public void insert(Query query, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    trainingDatasetEngine.insert(this, query.read(),
        writeOptions, overwrite ? SaveMode.Overwrite : SaveMode.Append);
  }

  public void insert(Dataset<Row> dataset, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    trainingDatasetEngine.insert(this, dataset,
        writeOptions, overwrite ? SaveMode.Overwrite : SaveMode.Append);
  }

  public Dataset<Row> read() throws FeatureStoreException {
    return read(new HashMap<>());
  }

  public Dataset<Row> read(Map<String, String> readOptions) {
    return trainingDatasetEngine.read(this, readOptions);
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    read(new HashMap<>()).show(numRows);
  }
}
