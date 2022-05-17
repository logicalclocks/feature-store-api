package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class TrainingDatasetRepository {

  @Getter
  private Integer version;
  private Map<String, Dataset<Row>> datasetSplits;
  private Dataset<Row> dataset;

  public TrainingDatasetRepository(Integer version, Dataset<Row> dataset) {
    this.version = version;
    this.dataset = dataset;
  }

  public TrainingDatasetRepository(Integer version,
      Map<String, Dataset<Row>> datasetSplits) {
    this.version = version;
    this.datasetSplits = datasetSplits;
  }

  @JsonIgnore
  public Dataset<Row> getDataset() {
    return dataset;
  }

  @JsonIgnore
  public Dataset<Row> getDataset(String split) {
    return datasetSplits.get(split);
  }
}
