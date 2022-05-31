package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TrainingDatasetBundle {

  @Getter
  private Integer version;
  private Map<String, Dataset<Row>> datasetSplits;
  private Dataset<Row> dataset;
  @Getter
  private String trainSplitName;
  private Boolean inMemory = true;

  public TrainingDatasetBundle(Integer version, Dataset<Row> dataset) {
    this.version = version;
    this.dataset = dataset;
  }

  public TrainingDatasetBundle(Integer version) {
    this.version = version;
    this.inMemory = false;
  }

  public TrainingDatasetBundle(Integer version, Map<String, Dataset<Row>> datasetSplits, String trainSplitName) {
    this.version = version;
    this.datasetSplits = datasetSplits;
    this.trainSplitName = trainSplitName;
  }

  @JsonIgnore
  public Dataset<Row> getDataset() {
    return dataset;
  }

  @JsonIgnore
  public Dataset<Row> getDataset(String split) {
    return datasetSplits.get(split);
  }

  @JsonIgnore
  public List<String> getSplitNames() {
    if (datasetSplits != null) {
      return new ArrayList<>(datasetSplits.keySet());
    } else {
      return Lists.newArrayList();
    }
  }

  @JsonIgnore
  public Dataset<Row> getTrainSet() {
    if (trainSplitName != null && !trainSplitName.isEmpty()) {
      return getDataset(trainSplitName);
    } else {
      return getDataset();
    }
  }
}
