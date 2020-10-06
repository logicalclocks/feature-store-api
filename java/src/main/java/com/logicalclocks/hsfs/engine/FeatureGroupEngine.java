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

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FeatureGroupEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private Utils utils = new Utils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);

  /**
   * Create the metadata and write the data to the online/offline feature store.
   *
   * @param featureGroup
   * @param dataset
   * @param primaryKeys
   * @param partitionKeys
   * @param storage
   * @param writeOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void saveFeatureGroup(FeatureGroup featureGroup, Dataset<Row> dataset, List<String> primaryKeys,
                               List<String> partitionKeys, Storage storage, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {

    if (featureGroup.getFeatureStore() != null) {
      featureGroup.setFeatures(utils.parseFeatureGroupSchema(dataset));
    }

    LOGGER.info("Featuregroup features: " + featureGroup.getFeatures());

    /* set primary features */
    if (primaryKeys != null) {
      primaryKeys.forEach(pk ->
          featureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPrimary(true);
            }
          }));
    }

    /* set partition key features */
    if (partitionKeys != null) {
      partitionKeys.forEach(pk ->
          featureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPartition(true);
            }
          }));
    }

    // Send Hopsworks the request to create a new feature group
    FeatureGroup apiFG = featureGroupApi.save(featureGroup);

    if (featureGroup.getVersion() == null) {
      LOGGER.info("VersionWarning: No version provided for creating feature group `" + featureGroup.getName()
          + "`, incremented version to `" + apiFG.getVersion() + "`.");
    }

    // Update the original object - Hopsworks returns the incremented version
    featureGroup.setId(apiFG.getId());
    featureGroup.setVersion(apiFG.getVersion());
    // TODO (davit): this must be Getter only.
    featureGroup.setLocation(apiFG.getLocation());
    featureGroup.setId(apiFG.getId());
    featureGroup.setCorrelations(apiFG.getCorrelations());
    featureGroup.setHistograms(apiFG.getHistograms());

    // Write the dataframe
    saveDataframe(featureGroup, dataset, storage,  SaveMode.Append,
            featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI
                    ? Constants.HUDI_BULK_INSERT : null, writeOptions);
  }

  public void saveDataframe(FeatureGroup featureGroup, Dataset<Row> dataset, Storage storage,
                            SaveMode saveMode, String operation, Map<String, String> writeOptions)
      throws IOException, FeatureStoreException {
    if (storage == null) {
      throw new FeatureStoreException("Storage not supported");
    }

    switch (storage) {
      case OFFLINE:
        saveOfflineDataframe(featureGroup, dataset, saveMode, operation, writeOptions);
        break;
      case ONLINE:
        saveOnlineDataframe(featureGroup, dataset, saveMode, writeOptions);
        break;
      case ALL:
        saveOfflineDataframe(featureGroup, dataset, saveMode, operation, writeOptions);
        saveOnlineDataframe(featureGroup, dataset, saveMode, writeOptions);
        break;
      default:
        throw new FeatureStoreException("Storage: " +  storage + " not recognized");
    }
  }

  /**
   * Write dataframe to the offline feature store.
   *
   * @param featureGroup
   * @param dataset
   * @param saveMode
   * @param operation
   * @param writeOptions
   */
  private void saveOfflineDataframe(FeatureGroup featureGroup, Dataset<Row> dataset,
                                    SaveMode saveMode, String operation, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {

    if (saveMode == SaveMode.Overwrite) {
      // If we set overwrite, then the directory will be removed and with it all the metadata
      // related to the feature group will be lost. We need to keep them.
      // So we call Hopsworks to manage to truncate the table and re-create the metadata
      // After that it's going to be just a normal append
      featureGroupApi.deleteContent(featureGroup);
      saveMode = SaveMode.Append;
    }

    SparkEngine.getInstance().writeOfflineDataframe(featureGroup, dataset, saveMode, operation, writeOptions);
  }

  private void saveOnlineDataframe(FeatureGroup featureGroup, Dataset<Row> dataset,
                                   SaveMode saveMode, Map<String, String> providedWriteOptions)
      throws IOException, FeatureStoreException {
    StorageConnector storageConnector = storageConnectorApi.getOnlineStorageConnector(featureGroup.getFeatureStore());
    Map<String, String> writeOptions =
        SparkEngine.getInstance().getOnlineOptions(providedWriteOptions, featureGroup, storageConnector);
    SparkEngine.getInstance().writeOnlineDataframe(dataset, saveMode, writeOptions);
  }

  public void updateStatisticsConfig(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    FeatureGroup apiFG = featureGroupApi.updateStatsConfig(featureGroup);
    featureGroup.setCorrelations(apiFG.getCorrelations());
    featureGroup.setHistograms(apiFG.getHistograms());
  }

  public FeatureGroupCommit[] commitDetails(FeatureGroup featureGroup, Integer limit)
      throws IOException, FeatureStoreException {
    FeatureGroupCommit[] commits = featureGroupApi.commitDetails(featureGroup, limit);
    return commits;
  }

  public FeatureGroupCommit commitDelete(FeatureGroup featureGroup, Dataset<Row> dataset)
      throws IOException, FeatureStoreException {
    FeatureGroupCommit featureGroupCommit = SparkEngine.getInstance().commitDelete(featureGroup, dataset);
    return featureGroupCommit;
  }
}
