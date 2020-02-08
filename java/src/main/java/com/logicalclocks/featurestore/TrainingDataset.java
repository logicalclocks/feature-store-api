package com.logicalclocks.featurestore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.logicalclocks.featurestore.metadata.Query;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
  @JsonIgnore
  private Query features;

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
  private String type = "hopsfsTrainingDatasetDTO";

  @Getter @Setter
  private int hopsfsConnectorId;

  @Getter @Setter
  private String hdfsStorePath;


  @Builder
  public TrainingDataset(Integer id, @NonNull String name, @NonNull Integer version, String description,
                         DataFormat dataFormat, Query features, Dataset<Row> featuresDataframe,
                         Map<String, String> writeOptions, StorageConnector storageConnector)
      throws FeatureStoreException{
    if (featuresDataframe == null && features == null) {
      throw new FeatureStoreException("Neither FeaturesDataFrame nor Features options was specified");
    } else if (featuresDataframe != null && features != null) {
      throw new FeatureStoreException("Both FeaturesDataFrame and Features options were specified");
    }

    this.id = id;
    this.name = name;
    this.version = version;
    this.description = description;
    this.dataFormat = dataFormat;
    this.features = features;
    this.featuresDataframe = featuresDataframe;
    this.writeOptions = writeOptions;
    this.hopsfsConnectorId = storageConnector.getId();
  }
}
