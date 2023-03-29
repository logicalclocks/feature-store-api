/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import com.logicalclocks.hsfs.Split;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

public class TrainingDatasetBundle {

  @Getter
  private Integer version;
  private Map<String, Dataset<Row>> datasetSplits;
  private Dataset<Row> dataset;
  private List<String> labels;
  private boolean hasSplit = false;
  private Boolean inMemory = true;

  public TrainingDatasetBundle(Integer version, Dataset<Row> dataset, List<String> labels) {
    this.version = version;
    this.dataset = dataset;
    this.labels = labels;
  }

  public TrainingDatasetBundle(Integer version) {
    this.version = version;
    this.inMemory = false;
  }

  public TrainingDatasetBundle(Integer version, Map<String, Dataset<Row>> datasetSplits, List<String> labels) {
    this.version = version;
    this.datasetSplits = datasetSplits;
    this.labels = labels;
    this.hasSplit = true;
  }

  @JsonIgnore
  public List<Dataset<Row>> getDataset(Boolean splitLabels) {
    if (inMemory) {
      if (hasSplit) {
        return getDataset(Split.TRAIN, splitLabels);
      } else {
        if (splitLabels) {
          return SparkEngine.splitLabels(dataset, labels);
        } else {
          return Lists.newArrayList(dataset);
        }
      }
    } else {
      return null;
    }
  }

  @JsonIgnore
  public List<Dataset<Row>> getDataset(String split, Boolean splitLabels) {
    if (inMemory) {
      if (splitLabels) {
        return SparkEngine.splitLabels(datasetSplits.get(split), labels);
      } else {
        return Lists.newArrayList(datasetSplits.get(split));
      }
    } else {
      return null;
    }
  }
}
