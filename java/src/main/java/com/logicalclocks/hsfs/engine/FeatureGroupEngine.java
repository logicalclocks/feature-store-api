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
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.engine.hudi.HudiEngine;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class FeatureGroupEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private HudiEngine hudiEngine = new HudiEngine();
  protected KafkaApi kafkaApi = new KafkaApi();

  private FeatureGroupUtils utils = new FeatureGroupUtils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);

  /**
   * Create the metadata and write the data to the online/offline feature store.
   *
   * @param featureGroup
   * @param dataset
   * @param partitionKeys
   * @param writeOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public FeatureGroup save(FeatureGroup featureGroup, Dataset<Row> dataset, List<String> partitionKeys,
                           String hudiPrecombineKey, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    dataset = utils.convertToDefaultDataframe(dataset);

    featureGroup = saveFeatureGroupMetaData(featureGroup, partitionKeys, hudiPrecombineKey,
        dataset, false);

    insert(featureGroup, dataset, null,
        featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI
            ? HudiOperationType.BULK_INSERT : null,
        SaveMode.Append, partitionKeys, hudiPrecombineKey, writeOptions);

    return featureGroup;
  }

  public void insert(FeatureGroup featureGroup, Dataset<Row> featureData, Storage storage,
                     HudiOperationType operation, SaveMode saveMode, List<String> partitionKeys,
                     String hudiPrecombineKey, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    Integer validationId = null;
    if (featureGroup.getId() == null) {
      featureGroup = saveFeatureGroupMetaData(featureGroup, partitionKeys,
          hudiPrecombineKey, featureData, false);
    }

    if (saveMode == SaveMode.Overwrite) {
      // If we set overwrite, then the directory will be removed and with it all the metadata
      // related to the feature group will be lost. We need to keep them.
      // So we call Hopsworks to manage to truncate the table and re-create the metadata
      // After that it's going to be just a normal append
      featureGroupApi.deleteContent(featureGroup);
    }

    saveDataframe(featureGroup, utils.convertToDefaultDataframe(featureData),
            storage, operation, writeOptions, utils.getKafkaConfig(featureGroup, writeOptions), validationId);
  }

  @Deprecated
  public StreamingQuery insertStream(FeatureGroup featureGroup, Dataset<Row> featureData, String queryName,
                                     String outputMode, boolean awaitTermination, Long timeout,
                                     String checkpointLocation, List<String> partitionKeys,
                                     String hudiPrecombineKey, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException, ParseException {

    if (!featureGroup.getOnlineEnabled()) {
      throw new FeatureStoreException("Online storage is not enabled for this feature group. "
          + "It is currently only possible to stream to the online storage.");
    }

    if (featureGroup.getId() == null) {
      featureGroup = saveFeatureGroupMetaData(featureGroup, partitionKeys,
          hudiPrecombineKey, featureData, true);
    }

    StreamingQuery streamingQuery = SparkEngine.getInstance().writeStreamDataframe(featureGroup,
        utils.convertToDefaultDataframe(featureData), queryName,
            outputMode, awaitTermination, timeout, checkpointLocation,
            utils.getKafkaConfig(featureGroup, writeOptions));

    return streamingQuery;
  }

  public void saveDataframe(FeatureGroup featureGroup, Dataset<Row> dataset, Storage storage,
                            HudiOperationType operation, Map<String, String> offlineWriteOptions,
                            Map<String, String> onlineWriteOptions, Integer validationId)
          throws IOException, FeatureStoreException, ParseException {
    if (!featureGroup.getOnlineEnabled() && storage == Storage.ONLINE) {
      throw new FeatureStoreException("Online storage is not enabled for this feature group. Set `online=false` to "
              + "write to the offline storage.");
    } else if (storage == Storage.OFFLINE || !featureGroup.getOnlineEnabled()) {
      SparkEngine.getInstance().writeOfflineDataframe(featureGroup, dataset, operation,
              offlineWriteOptions, validationId);
    } else if (storage == Storage.ONLINE) {
      SparkEngine.getInstance().writeOnlineDataframe(featureGroup, dataset, featureGroup.getOnlineTopicName(),
              onlineWriteOptions);
    } else if (featureGroup.getOnlineEnabled() && storage == null) {
      SparkEngine.getInstance().writeOfflineDataframe(featureGroup, dataset, operation,
              offlineWriteOptions, validationId);
      SparkEngine.getInstance().writeOnlineDataframe(featureGroup, dataset, featureGroup.getOnlineTopicName(),
              onlineWriteOptions);
    } else {
      throw new FeatureStoreException("Error writing to offline and online feature store.");
    }
  }

  public FeatureGroup saveFeatureGroupMetaData(FeatureGroup featureGroup, List<String> partitionKeys,
                                               String hudiPrecombineKey, Dataset<Row> featureData,
                                               boolean saveEmpty)
      throws FeatureStoreException, IOException, ParseException {

    if (featureGroup.getFeatures() == null) {
      featureGroup.setFeatures(utils.parseFeatureGroupSchema(featureData,
          featureGroup.getTimeTravelFormat()));
    }

    LOGGER.info("Featuregroup features: " + featureGroup.getFeatures());

    // verify primary, partition, event time or hudi precombine keys
    utils.verifyAttributeKeyNames(featureGroup, partitionKeys, hudiPrecombineKey);

    /* set primary features */
    if (featureGroup.getPrimaryKeys() != null) {
      featureGroup.getPrimaryKeys().forEach(pk ->
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

    /* set hudi precombine key name */
    if (hudiPrecombineKey != null) {
      featureGroup.getFeatures().forEach(f -> {
        if (f.getName().equals(hudiPrecombineKey)) {
          f.setHudiPrecombineKey(true);
        }
      });
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
    featureGroup.setStatisticsConfig(apiFG.getStatisticsConfig());
    featureGroup.setOnlineTopicName(apiFG.getOnlineTopicName());

    /* if hudi precombine key was not provided and TimeTravelFormat is HUDI, retrieve from backend and set */
    if (featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI && hudiPrecombineKey == null) {
      List<Feature> features = apiFG.getFeatures();
      featureGroup.setFeatures(features);
    }

    if (saveEmpty) {
      // insertStream method was called on feature group object that has not been saved
      // we will use writeOfflineDataframe method on empty dataframe to create directory structure
      SparkEngine.getInstance().writeOfflineDataframe(featureGroup,
          SparkEngine.getInstance().createEmptyDataFrame(featureData),
          featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI
              ? HudiOperationType.BULK_INSERT : null,
          null, null);
    }

    return featureGroup;
  }
}
