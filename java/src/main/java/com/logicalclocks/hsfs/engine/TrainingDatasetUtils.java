/*
 * Copyright (c) 2022 Logical Clocks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.TrainingDatasetType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class TrainingDatasetUtils {

  public static void setLabelFeature(List<TrainingDatasetFeature> features, List<String> labels)
      throws FeatureStoreException {
    if (labels != null && !labels.isEmpty()) {
      for (String label : labels) {
        Optional<TrainingDatasetFeature> feature =
            features.stream().filter(f -> f.getName().equals(label)).findFirst();
        if (feature.isPresent()) {
          feature.get().setLabel(true);
        } else {
          throw new FeatureStoreException("The specified label `" + label + "` could not be found among the features: "
              + features.stream().map(TrainingDatasetFeature::getName) + ".");
        }
      }
    }
  }


  public List<TrainingDatasetFeature> parseTrainingDatasetSchema(Dataset<Row> dataset) throws FeatureStoreException {
    List<TrainingDatasetFeature> features = new ArrayList<>();

    int index = 0;
    for (StructField structField : dataset.schema().fields()) {
      // TODO(Fabio): unit test this one for complext types
      features.add(new TrainingDatasetFeature(
          structField.name().toLowerCase(), structField.dataType().catalogString(), index++));
    }

    return features;
  }

  public void trainingDatasetSchemaMatch(Dataset<Row> dataset, List<TrainingDatasetFeature> features)
      throws FeatureStoreException {
    StructType tdStructType = new StructType(features.stream()
        .sorted(Comparator.comparingInt(TrainingDatasetFeature::getIndex))
        .map(f -> new StructField(f.getName(),
            // What should we do about the nullables
            new CatalystSqlParser().parseDataType(f.getType()), true, Metadata.empty())
        ).toArray(StructField[]::new));

    if (!dataset.schema().equals(tdStructType)) {
      throw new FeatureStoreException("The Dataframe schema: " + dataset.schema()
          + " does not match the training dataset schema: " + tdStructType);
    }
  }

  public TrainingDatasetType getTrainingDatasetType(StorageConnector storageConnector) {
    if (storageConnector == null) {
      return TrainingDatasetType.HOPSFS_TRAINING_DATASET;
    } else if (storageConnector.getStorageConnectorType() == StorageConnectorType.HOPSFS) {
      return TrainingDatasetType.HOPSFS_TRAINING_DATASET;
    } else {
      return TrainingDatasetType.EXTERNAL_TRAINING_DATASET;
    }
  }

}
