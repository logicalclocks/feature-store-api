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
import com.logicalclocks.hsfs.constructor.FsQuery;
import com.logicalclocks.hsfs.metadata.TagsApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrainingDatasetEngine {

  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.TRAINING_DATASET);
  private TrainingDatasetUtils utils = new TrainingDatasetUtils();


  private static final Logger LOGGER = LoggerFactory.getLogger(TrainingDatasetEngine.class);

  /**
   * Make a REST call to Hopsworks to create the metadata and write the data on the File System.
   *
   * @param trainingDataset
   * @param dataset
   * @param userWriteOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public TrainingDataset save(TrainingDataset trainingDataset, Dataset<Row> dataset,
      Map<String, String> userWriteOptions, List<String> labels)
      throws FeatureStoreException, IOException {

    trainingDataset.setFeatures(utils.parseTrainingDatasetSchema(dataset));

    // set label features
    TrainingDatasetUtils.setLabelFeature(trainingDataset.getFeatures(), labels);

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
    trainingDataset.setStorageConnector(apiTD.getStorageConnector());

    // Build write options map
    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(userWriteOptions, trainingDataset.getDataFormat());

    SparkEngine.getInstance().write(trainingDataset, dataset, writeOptions, SaveMode.Overwrite);

    return trainingDataset;
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
      throws FeatureStoreException, IOException {
    // validate that the schema matches
    utils.trainingDatasetSchemaMatch(dataset, trainingDataset.getFeatures());

    // check if this training dataset has transformation functions attached and throw exception if any
    if (trainingDatasetApi.getTransformationFunctions(trainingDataset).size() > 0) {
      throw new FeatureStoreException("This training dataset has transformation functions attached and "
          + "insert operation must be performed from a PySpark application");
    }

    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(providedOptions, trainingDataset.getDataFormat());

    SparkEngine.getInstance().write(trainingDataset, dataset, writeOptions, saveMode);
  }

  public Dataset<Row> read(TrainingDataset trainingDataset, String split, Map<String, String> providedOptions)
      throws FeatureStoreException, IOException {
    Map<String, String> readOptions =
        SparkEngine.getInstance().getReadOptions(providedOptions, trainingDataset.getDataFormat());

    String path = null;
    if (!com.google.common.base.Strings.isNullOrEmpty(split)) {
      path = new Path(trainingDataset.getLocation(), split).toString();
    } else {
      path = new Path(trainingDataset.getLocation(), trainingDataset.getName()).toString();
    }
    return (Dataset<Row>) trainingDataset.getStorageConnector()
        .read(null, trainingDataset.getDataFormat().toString(), readOptions, path);
  }

  public void addTag(TrainingDataset trainingDataset, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(trainingDataset, name, value);
  }

  public Map<String, Object> getTags(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    return tagsApi.get(trainingDataset);
  }

  public Object getTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    return tagsApi.get(trainingDataset, name);
  }

  public void deleteTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    tagsApi.deleteTag(trainingDataset, name);
  }

  public String getQuery(TrainingDataset trainingDataset, Storage storage, boolean withLabel, boolean isHiveQuery)
      throws FeatureStoreException, IOException {
    FsQuery fsQuery = trainingDatasetApi.getQuery(trainingDataset, withLabel, isHiveQuery);

    if (storage == Storage.OFFLINE) {
      // register the temporary tables so that people can make
      // batch inference requests by doing `fs.sql(td.getQuery())`
      fsQuery.registerOnDemandFeatureGroups();
      fsQuery.registerHudiFeatureGroups(new HashMap<>());
    }

    return fsQuery.getStorageQuery(storage);
  }

  public void updateStatisticsConfig(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    TrainingDataset apiTD = trainingDatasetApi.updateMetadata(trainingDataset, "updateStatsConfig");
    trainingDataset.getStatisticsConfig().setCorrelations(apiTD.getStatisticsConfig().getCorrelations());
    trainingDataset.getStatisticsConfig().setHistograms(apiTD.getStatisticsConfig().getHistograms());
    trainingDataset.getStatisticsConfig().setExactUniqueness(apiTD.getStatisticsConfig().getExactUniqueness());
  }

  public void delete(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    trainingDatasetApi.delete(trainingDataset);
  }


}
