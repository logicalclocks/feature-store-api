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
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.TagsApi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureGroupEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);
  private HudiEngine hudiEngine = new HudiEngine();
  private Utils utils = new Utils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);

  /**
   * Create the metadata and write the data to the online/offline feature store.
   *
   * @param featureGroup
   * @param dataset
   * @param primaryKeys
   * @param partitionKeys
   * @param writeOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void saveFeatureGroup(FeatureGroup featureGroup, Dataset<Row> dataset, List<String> primaryKeys,
                               List<String> partitionKeys, Map<String, String> writeOptions)
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
    featureGroup.setLocation(apiFG.getLocation());
    featureGroup.setId(apiFG.getId());
    featureGroup.setCorrelations(apiFG.getCorrelations());
    featureGroup.setHistograms(apiFG.getHistograms());

    // Write the dataframe
    saveDataframe(featureGroup, dataset, null,  SaveMode.Append,
            featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI
                    ? HudiOperationType.BULK_INSERT : null, writeOptions);
  }

  public void saveDataframe(FeatureGroup featureGroup, Dataset<Row> dataset, Storage storage,
                            SaveMode saveMode, HudiOperationType operation, Map<String, String> writeOptions)
      throws IOException, FeatureStoreException {
    if (!featureGroup.getOnlineEnabled() && storage == Storage.ONLINE) {
      throw new FeatureStoreException("Online storage is not enabled for this feature group. Set `online=false` to "
        + "write to the offline storage.");
    } else if (storage == Storage.OFFLINE || !featureGroup.getOnlineEnabled()) {
      saveOfflineDataframe(featureGroup, dataset, saveMode, operation, writeOptions);
    } else if (storage == Storage.ONLINE) {
      saveOnlineDataframe(featureGroup, dataset, saveMode, writeOptions);
    } else if (featureGroup.getOnlineEnabled() && storage == null) {
      saveOfflineDataframe(featureGroup, dataset, saveMode, operation, writeOptions);
      saveOnlineDataframe(featureGroup, dataset, saveMode, writeOptions);
    } else {
      throw new FeatureStoreException("Error writing to offline and online feature store.");
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
  private void saveOfflineDataframe(FeatureGroup featureGroup, Dataset<Row> dataset, SaveMode saveMode,
                                    HudiOperationType operation, Map<String, String> writeOptions)
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

  public void delete(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    featureGroupApi.delete(featureGroup);
  }

  public void addTag(FeatureGroup featureGroup, String name, String value) throws FeatureStoreException, IOException {
    tagsApi.add(featureGroup, name, value);
  }

  public Map<String, String> getTag(FeatureGroup featureGroup, String name) throws FeatureStoreException, IOException {
    return tagsApi.get(featureGroup, name);
  }

  public void deleteTag(FeatureGroup featureGroup, String name) throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureGroup, name);
  }

  public void updateStatisticsConfig(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    FeatureGroup apiFG = featureGroupApi.updateMetadata(featureGroup, "updateStatsSettings");
    featureGroup.setCorrelations(apiFG.getCorrelations());
    featureGroup.setHistograms(apiFG.getHistograms());
  }

  public Map<String, Map<String,String>> commitDetails(FeatureGroup featureGroup, Integer limit)
      throws IOException, FeatureStoreException {
    List<FeatureGroupCommit> featureGroupCommits = featureGroupApi.commitDetails(featureGroup, limit);
    if (featureGroupCommits == null) {
      throw new FeatureStoreException("There are no commit details available for this Feature group");
    }
    Map<String, Map<String,String>> commitDetails = new HashMap<String, Map<String,String>>();
    for (FeatureGroupCommit featureGroupCommit : featureGroupCommits) {
      commitDetails.put(featureGroupCommit.getCommitID().toString(), new HashMap<String, String>() {{
            put("committedOn", hudiEngine.timeStampToHudiFormat(featureGroupCommit.getCommitID()));
            put("rowsUpdated", featureGroupCommit.getRowsUpdated().toString());
            put("rowsInserted", featureGroupCommit.getRowsInserted().toString());
            put("rowsDeleted", featureGroupCommit.getRowsDeleted().toString());
          }}
      );
    }
    return commitDetails;
  }

  public FeatureGroupCommit commitDelete(FeatureGroup featureGroup, Dataset<Row> dataset,
                                         Map<String, String> writeOptions) throws IOException, FeatureStoreException {
    return hudiEngine.deleteRecord(SparkEngine.getInstance().getSparkSession(), featureGroup, dataset, writeOptions);
  }

  public void updateDescription(FeatureGroup featureGroup, String description)
      throws FeatureStoreException, IOException {
    FeatureGroup apiFG = featureGroupApi.updateMetadata(featureGroup.withDescription(description), "updateMetadata");
    featureGroup.setDescription(apiFG.getDescription());
  }

  public void appendFeatures(FeatureGroup featureGroup, List<Feature> features)
      throws FeatureStoreException, IOException {
    features.addAll(featureGroup.getFeatures());
    FeatureGroup apiFG = featureGroupApi.updateMetadata(featureGroup.withFeatures(features), "updateMetadata");
    featureGroup.setFeatures(apiFG.getFeatures());
  }
}
