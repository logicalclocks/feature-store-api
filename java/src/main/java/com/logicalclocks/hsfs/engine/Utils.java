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
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

  StorageConnectorApi storageConnectorApi = new StorageConnectorApi();

  public List<Feature> parseFeatureGroupSchema(Dataset<Row> dataset) throws FeatureStoreException {
    List<Feature> features = new ArrayList<>();
    for (StructField structField : dataset.schema().fields()) {
      // TODO(Fabio): unit test this one for complext types
      Feature f = new Feature(structField.name(), structField.dataType().catalogString(), false, false);
      if (structField.metadata().contains("description")) {
        f.setDescription(structField.metadata().getString("description"));
      }
      features.add(f);
    }

    return features;
  }

  public List<TrainingDatasetFeature> parseTrainingDatasetSchema(Dataset<Row> dataset) throws FeatureStoreException {
    List<TrainingDatasetFeature> features = new ArrayList<>();

    int index = 0;
    for (StructField structField : dataset.schema().fields()) {
      // TODO(Fabio): unit test this one for complext types
      features.add(new TrainingDatasetFeature(structField.name(), structField.dataType().catalogString(), index++));
    }

    return features;
  }

  public void trainingDatasetSchemaMatch(Dataset<Row> dataset, List<TrainingDatasetFeature> features)
      throws FeatureStoreException {
    StructType tdStructType = new StructType(features.stream()
        .sorted(Comparator.comparingInt(TrainingDatasetFeature::getIndex))
        .map(f -> new StructField(f.getName(),
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

  public String getHiveMetastoreConnector(FeatureGroup featureGroup) throws IOException, FeatureStoreException {
    StorageConnector storageConnector = storageConnectorApi.getByNameAndType(featureGroup.getFeatureStore(),
        featureGroup.getFeatureStore().getName(), StorageConnectorType.JDBC);
    String connStr = storageConnector.getConnectionString();
    String pw = FileUtils.readFileToString(new File("material_passwd"));
    return connStr + "sslTrustStore=t_certificate;trustStorePassword=" + pw
        + ";sslKeyStore=k_certificate;keyStorePassword=" + pw;
  }
}
