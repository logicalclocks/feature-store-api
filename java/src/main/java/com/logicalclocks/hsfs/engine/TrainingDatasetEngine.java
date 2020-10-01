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

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.metadata.TagsApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TrainingDatasetEngine {

  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.TRAINING_DATASET);
  private Utils utils = new Utils();

  private static final Logger LOGGER = LoggerFactory.getLogger(TrainingDatasetEngine.class);
  //TODO:
  //      Compute statistics

  /**
   * Make a REST call to Hopsworks to create the metadata and write the data on the File System.
   *
   * @param trainingDataset
   * @param dataset
   * @param userWriteOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save(TrainingDataset trainingDataset, Dataset<Row> dataset,
                     Map<String, String> userWriteOptions)
      throws FeatureStoreException, IOException {

    if (trainingDataset.getQueryInt() == null) {
      // if the training dataset hasn't been generated from a query, parse the schema and set the features
      trainingDataset.setFeatures(utils.parseTrainingDatasetSchema(dataset));
    }

    // Make the rest call to create the training dataset metadata
    TrainingDataset apiTD = trainingDatasetApi.createTrainingDataset(trainingDataset);

    if (trainingDataset.getVersion() == null) {
      LOGGER.info("VersionWarning: No version provided for creating training dataset `" + trainingDataset.getName()
          + "`, incremented version to `" + apiTD.getVersion() + "`.");
    }

    // Update the original object - Hopsworks returns the full location and incremented version
    trainingDataset.setLocation(apiTD.getLocation());
    trainingDataset.setVersion(apiTD.getVersion());
    trainingDataset.setId(apiTD.getId());

    // Build write options map
    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(userWriteOptions, trainingDataset.getDataFormat());

    SparkEngine.getInstance().write(trainingDataset, dataset, writeOptions, SaveMode.Overwrite);
  }

  /**
   * Insert (append or overwrite) data on a training dataset.
   *
   * @param trainingDataset
   * @param dataset
   * @param providedOptions
   * @param saveMode
   * @throws FeatureStoreException
   */
  public void insert(TrainingDataset trainingDataset, Dataset<Row> dataset,
                     Map<String, String> providedOptions, SaveMode saveMode)
      throws FeatureStoreException {
    // validate that the schema matches
    utils.trainingDatasetSchemaMatch(dataset, trainingDataset.getFeatures());

    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(providedOptions, trainingDataset.getDataFormat());

    SparkEngine.getInstance().write(trainingDataset, dataset, writeOptions, saveMode);
  }

  public Dataset<Row> read(TrainingDataset trainingDataset, String split, Map<String, String> providedOptions) {
    if (trainingDataset.getStorageConnector() != null) {
      SparkEngine.getInstance().configureConnector(trainingDataset.getStorageConnector());
    }

    String path = "";
    if (com.google.common.base.Strings.isNullOrEmpty(split)) {
      // ** glob means "all sub directories"
      path = new Path(trainingDataset.getLocation(), "**").toString();
    } else {
      path = new Path(trainingDataset.getLocation(), split).toString();
    }

    Map<String, String> readOptions =
        SparkEngine.getInstance().getReadOptions(providedOptions, trainingDataset.getDataFormat());
    return SparkEngine.getInstance().read(trainingDataset.getDataFormat(), readOptions, path);
  }

  public void addTag(TrainingDataset trainingDataset, String name, String value)
      throws FeatureStoreException, IOException {
    tagsApi.add(trainingDataset, name, value);
  }

  public Map<String, String> getTag(TrainingDataset trainingDataset, String name)
      throws FeatureStoreException, IOException {
    return tagsApi.get(trainingDataset, name);
  }

  public void deleteTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    tagsApi.deleteTag(trainingDataset, name);
  }

  public String getQuery(TrainingDataset trainingDataset, Storage storage)
      throws FeatureStoreException, IOException {
    return trainingDatasetApi.getQuery(trainingDataset).getStorageQuery(storage);
  }
}
