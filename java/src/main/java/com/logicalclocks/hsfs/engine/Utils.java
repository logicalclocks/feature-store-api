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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureGroup;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

  // TODO(Fabio): make sure we keep save the feature store/feature group for serving
  public List<Feature> parseSchema(Dataset<Row> dataset) throws FeatureStoreException {
    List<Feature> features = new ArrayList<>();
    for (StructField structField : dataset.schema().fields()) {
      // TODO(Fabio): unit test this one for complext types
      features.add(new Feature(structField.name(), structField.dataType().catalogString(),
          structField.dataType().catalogString(), false, false, false));
    }

    return features;
  }

  // TODO(Fabio): keep into account the sorting - needs fixing in Hopsworks as well
  public void schemaMatches(Dataset<Row> dataset, List<Feature> features) throws FeatureStoreException {
    StructType tdStructType = new StructType(features.stream().map(
        f -> new StructField(f.getName(),
            // What should we do about the nullables
            new CatalystSqlParser(null).parseDataType(f.getType()), true, Metadata.empty())
    ).toArray(StructField[]::new));

    if (!dataset.schema().equals(tdStructType)) {
      throw new FeatureStoreException("The Dataframe schema: " + dataset.schema()
          + " does not match the training dataset schema: " + tdStructType);
    }
  }

  // TODO(Fabio): this should be moved in the backend
  public String getTableName(FeatureGroup offlineFeatureGroup) {
    return offlineFeatureGroup.getFeatureStore().getName() + "."
        + offlineFeatureGroup.getName() + "_" + offlineFeatureGroup.getVersion();
  }

  public Seq<String> getPartitionColumns(FeatureGroup offlineFeatureGroup) {
    List<String> partitionCols = offlineFeatureGroup.getFeatures().stream()
        .filter(Feature::getPartition)
        .map(Feature::getName)
        .collect(Collectors.toList());


    return JavaConverters.asScalaIteratorConverter(partitionCols.iterator()).asScala().toSeq();
  }

  public Seq<String> getPrimaryColumns(FeatureGroup offlineFeatureGroup) {
    List<String> primaryCols = offlineFeatureGroup.getFeatures().stream()
            .filter(Feature::getPrimary)
            .map(Feature::getName)
            .collect(Collectors.toList());

    return JavaConverters.asScalaIteratorConverter(primaryCols.iterator()).asScala().toSeq();
  }

  public String getFgName(FeatureGroup featureGroup) {
    return featureGroup.getName() + "_" + featureGroup.getVersion();
  }

}
