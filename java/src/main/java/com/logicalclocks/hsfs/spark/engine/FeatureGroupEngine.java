/*
 *  Copyright (c) 2020-2022. Logical Clocks AB
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

package com.logicalclocks.hsfs.spark.engine;

import com.logicalclocks.hsfs.generic.Feature;
import com.logicalclocks.hsfs.generic.FeatureGroupCommit;
import com.logicalclocks.hsfs.generic.FeatureStore;
import com.logicalclocks.hsfs.generic.JobConfiguration;
import com.logicalclocks.hsfs.generic.StatisticsConfig;
import com.logicalclocks.hsfs.spark.StreamFeatureGroup;
import com.logicalclocks.hsfs.generic.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.spark.FeatureGroup;
import com.logicalclocks.hsfs.generic.FeatureStoreException;
import com.logicalclocks.hsfs.generic.HudiOperationType;
import com.logicalclocks.hsfs.generic.Storage;
import com.logicalclocks.hsfs.generic.TimeTravelFormat;
import com.logicalclocks.hsfs.spark.engine.hudi.HudiEngine;
import com.logicalclocks.hsfs.generic.metadata.KafkaApi;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupApi;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
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
    dataset = SparkEngine.getInstance().sanitizeFeatureNames(dataset);

    featureGroup = saveFeatureGroupMetaData(featureGroup, partitionKeys, hudiPrecombineKey,
        dataset, false);

    insert(featureGroup, dataset, null,
        featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI
            ? HudiOperationType.BULK_INSERT : null,
        SaveMode.Append, partitionKeys, hudiPrecombineKey, writeOptions);

    return featureGroup;
  }

  /**
   * Create the metadata and write the data to the online/offline feature store.
   *
   * @param featureGroup
   * @param dataset
   * @param partitionKeys
   * @param writeOptions
   * @param sparkJobConfiguration
   * @throws FeatureStoreException
   * @throws IOException
   */
  public StreamFeatureGroup save(StreamFeatureGroup featureGroup, Dataset<Row> dataset, List<String> partitionKeys,
                                 String hudiPrecombineKey, Map<String, String> writeOptions,
                                 JobConfiguration sparkJobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

    StreamFeatureGroup updatedFeatureGroup = saveFeatureGroupMetaData(featureGroup, partitionKeys, hudiPrecombineKey,
        writeOptions, sparkJobConfiguration, dataset);

    insert(updatedFeatureGroup, SparkEngine.getInstance().sanitizeFeatureNames(dataset),
        com.logicalclocks.hsfs.generic.SaveMode.APPEND,  partitionKeys, hudiPrecombineKey, writeOptions,
        sparkJobConfiguration);

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

    saveDataframe(featureGroup, featureData, storage, operation,
        writeOptions, SparkEngine.getInstance().getKafkaConfig(featureGroup, writeOptions), validationId);
  }

  public void insert(StreamFeatureGroup streamFeatureGroup, Dataset<Row> featureData,
                     com.logicalclocks.hsfs.generic.SaveMode saveMode, List<String> partitionKeys,
                     String hudiPrecombineKey, Map<String, String> writeOptions, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

    if (streamFeatureGroup.getId() == null) {
      streamFeatureGroup = saveFeatureGroupMetaData(streamFeatureGroup, partitionKeys, hudiPrecombineKey, writeOptions,
          jobConfiguration, featureData);
    }

    if (saveMode == com.logicalclocks.hsfs.generic.SaveMode.OVERWRITE) {
      // If we set overwrite, then the directory will be removed and with it all the metadata
      // related to the feature group will be lost. We need to keep them.
      // So we call Hopsworks to manage to truncate the table and re-create the metadata
      // After that it's going to be just a normal append
      featureGroupApi.deleteContent(streamFeatureGroup);
    }

    SparkEngine.getInstance().writeOnlineDataframe(streamFeatureGroup, featureData,
        streamFeatureGroup.getOnlineTopicName(),
        SparkEngine.getInstance().getKafkaConfig(streamFeatureGroup, writeOptions));
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
        SparkEngine.getInstance().sanitizeFeatureNames(featureData), queryName, outputMode, awaitTermination, timeout,
        checkpointLocation, SparkEngine.getInstance().getKafkaConfig(featureGroup, writeOptions));

    return streamingQuery;
  }

  @SneakyThrows
  public StreamingQuery insertStream(StreamFeatureGroup streamFeatureGroup, Dataset<Row> featureData, String queryName,
                                     String outputMode, boolean awaitTermination, Long timeout,
                                     String checkpointLocation, List<String> partitionKeys, String hudiPrecombineKey,
                                     Map<String, String> writeOptions, JobConfiguration jobConfiguration) {

    if (writeOptions == null) {
      writeOptions = new HashMap<>();
    }

    if (streamFeatureGroup.getId() == null) {
      streamFeatureGroup = saveFeatureGroupMetaData(streamFeatureGroup, partitionKeys, hudiPrecombineKey, writeOptions,
          jobConfiguration, featureData);
    }

    return SparkEngine.getInstance().writeStreamDataframe(streamFeatureGroup,
        SparkEngine.getInstance().sanitizeFeatureNames(featureData), queryName, outputMode, awaitTermination, timeout,
        checkpointLocation, SparkEngine.getInstance().getKafkaConfig(streamFeatureGroup, writeOptions));
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
      featureGroup.setFeatures(SparkEngine.getInstance().parseFeatureGroupSchema(featureData,
          featureGroup.getTimeTravelFormat()));
    }

    LOGGER.info("Featuregroup features: " + featureGroup.getFeatures());

    FeatureGroup apiFG = (FeatureGroup) featureGroupApi.saveFeatureGroupMetaData(featureGroup, partitionKeys,
        hudiPrecombineKey, null, null);

    featureGroup.setOnlineTopicName(apiFG.getOnlineTopicName());

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

  public StreamFeatureGroup saveFeatureGroupMetaData(StreamFeatureGroup featureGroup, List<String> partitionKeys,
                                                     String hudiPrecombineKey, Map<String, String> writeOptions,
                                                     JobConfiguration sparkJobConfiguration,
                                                     Dataset<Row> featureData)
      throws FeatureStoreException, IOException {

    if (featureGroup.getFeatures() == null) {
      featureGroup.setFeatures(SparkEngine.getInstance()
          .parseFeatureGroupSchema(SparkEngine.getInstance().sanitizeFeatureNames(featureData),
              featureGroup.getTimeTravelFormat()));
    }

    LOGGER.info("Featuregroup features: " + featureGroup.getFeatures());

    StreamFeatureGroup apiFG = (StreamFeatureGroup) featureGroupApi.saveFeatureGroupMetaData(featureGroup,
        partitionKeys, hudiPrecombineKey, writeOptions, sparkJobConfiguration);
    featureGroup.setOnlineTopicName(apiFG.getOnlineTopicName());


    return featureGroup;
  }


  public FeatureGroup getOrCreateFeatureGroup(FeatureStore featureStore, String name, Integer version,
                                              String description, List<String> primaryKeys, List<String> partitionKeys,
                                              String hudiPrecombineKey, boolean onlineEnabled,
                                              TimeTravelFormat timeTravelFormat,
                                              StatisticsConfig statisticsConfig, String eventTime)
      throws IOException, FeatureStoreException {


    FeatureGroup featureGroup;
    try {
      featureGroup =  (FeatureGroup) getFeatureGroup(featureStore, name, version);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270009")) {
        featureGroup =  FeatureGroup.builder()
            .featureStore(featureStore)
            .name(name)
            .version(version)
            .description(description)
            .primaryKeys(primaryKeys)
            .partitionKeys(partitionKeys)
            .hudiPrecombineKey(hudiPrecombineKey)
            .onlineEnabled(onlineEnabled)
            .timeTravelFormat(timeTravelFormat)
            .statisticsConfig(statisticsConfig)
            .eventTime(eventTime)
            .build();

        featureGroup.setFeatureStore(featureStore);
      } else {
        throw e;
      }
    }

    return featureGroup;
  }

  public <T extends FeatureGroupBase> void appendFeatures(FeatureGroupBase featureGroupBase, List<Feature> features,
                                                          Class<T> fgClass)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupBase.getFeatures().addAll(features);
    T apiFG = featureGroupApi.updateMetadata(featureGroupBase, "updateMetadata",
        fgClass);
    featureGroupBase.setFeatures(apiFG.getFeatures());
    if (featureGroupBase instanceof FeatureGroup) {
      FeatureGroup featureGroup = (FeatureGroup) featureGroupBase;
      SparkEngine.getInstance().writeOfflineDataframe(featureGroup,
          SparkEngine.getInstance().getEmptyAppendedDataframe(featureGroup.read(), features),
          HudiOperationType.UPSERT, new HashMap<>(), null);
    }
  }

  public Map<Long, Map<String, String>> commitDetails(FeatureGroupBase featureGroupBase, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    // operation is only valid for time travel enabled feature group
    if (!((featureGroupBase instanceof FeatureGroup && featureGroupBase.getTimeTravelFormat() == TimeTravelFormat.HUDI)
        || featureGroupBase instanceof StreamFeatureGroup)) {
      // operation is only valid for time travel enabled feature group
      throw new FeatureStoreException("commitDetails function is only valid for "
          + "time travel enabled feature group");
    }
    return utils.getCommitDetails(featureGroupBase, null, limit);
  }

  public Map<Long, Map<String, String>> commitDetailsByWallclockTime(FeatureGroupBase featureGroup,
                                                                     String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return utils.getCommitDetails(featureGroup, wallclockTime, limit);
  }

  public FeatureGroupCommit commitDelete(FeatureGroupBase featureGroupBase, Dataset<Row> genericDataset,
                                         Map<String, String> writeOptions)
      throws IOException, FeatureStoreException, ParseException {
    if (!((featureGroupBase instanceof FeatureGroup && featureGroupBase.getTimeTravelFormat() == TimeTravelFormat.HUDI)
        || featureGroupBase instanceof StreamFeatureGroup)) {
      // operation is only valid for time travel enabled feature group
      throw new FeatureStoreException("delete function is only valid for "
          + "time travel enabled feature group");
    }
    HudiEngine hudiEngine = new HudiEngine();
    return hudiEngine.deleteRecord(SparkEngine.getInstance().getSparkSession(), featureGroupBase, genericDataset,
        writeOptions);
  }

  public List<FeatureGroup> getFeatureGroups(FeatureStore featureStore, String fgName)
      throws FeatureStoreException, IOException {
    FeatureGroup[] offlineFeatureGroups =
       featureGroupApi.getInternal(featureStore, fgName, null, FeatureGroup[].class);

    return Arrays.asList(offlineFeatureGroups);
  }

  public FeatureGroup getFeatureGroup(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    FeatureGroup[] offlineFeatureGroups =
        featureGroupApi.getInternal(featureStore, fgName, fgVersion, FeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    FeatureGroup resultFg = offlineFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }
}
