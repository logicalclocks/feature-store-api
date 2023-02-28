/*
 * Copyright (c) 2021. Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.DeltaStreamerJobConf;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.JobConfiguration;
import com.logicalclocks.hsfs.SaveMode;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.Option;

import lombok.SneakyThrows;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamFeatureGroupEngine {

  private KafkaApi kafkaApi = new KafkaApi();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private FeatureGroupUtils utils = new FeatureGroupUtils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);

  public <S> StreamFeatureGroup save(StreamFeatureGroup featureGroup, S dataset, List<String> partitionKeys,
                                     String hudiPrecombineKey, Map<String, String> writeOptions,
                                     JobConfiguration sparkJobConfiguration)
          throws FeatureStoreException, IOException, ParseException {

    StreamFeatureGroup updatedFeatureGroup = saveFeatureGroupMetaData(featureGroup, partitionKeys, hudiPrecombineKey,
        writeOptions, sparkJobConfiguration, dataset);

    insert(updatedFeatureGroup, utils.convertToDefaultDataframe(dataset),
        SaveMode.APPEND,  partitionKeys, hudiPrecombineKey, writeOptions, sparkJobConfiguration);

    return featureGroup;
  }

  @SneakyThrows
  public <S> Object insertStream(StreamFeatureGroup streamFeatureGroup, S featureData, String queryName,
                                 String outputMode, boolean awaitTermination, Long timeout,  String checkpointLocation,
                                 List<String> partitionKeys, String hudiPrecombineKey, Map<String, String>
                                       writeOptions, JobConfiguration jobConfiguration) {

    if (writeOptions == null) {
      writeOptions = new HashMap<>();
    }

    if (streamFeatureGroup.getId() == null) {
      streamFeatureGroup = saveFeatureGroupMetaData(streamFeatureGroup, partitionKeys, hudiPrecombineKey, writeOptions,
          jobConfiguration, featureData);
    }

    return SparkEngine.getInstance().writeStreamDataframe(streamFeatureGroup,
      utils.convertToDefaultDataframe(featureData), queryName, outputMode,
            awaitTermination, timeout, checkpointLocation, utils.getKafkaConfig(streamFeatureGroup, writeOptions));
  }

  public <S> void insert(StreamFeatureGroup streamFeatureGroup, S featureData,
                         SaveMode saveMode, List<String> partitionKeys, String hudiPrecombineKey,
                         Map<String, String> writeOptions, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

    if (streamFeatureGroup.getId() == null) {
      streamFeatureGroup = saveFeatureGroupMetaData(streamFeatureGroup, partitionKeys, hudiPrecombineKey, writeOptions,
          jobConfiguration, featureData);
    }

    if (saveMode == SaveMode.OVERWRITE) {
      // If we set overwrite, then the directory will be removed and with it all the metadata
      // related to the feature group will be lost. We need to keep them.
      // So we call Hopsworks to manage to truncate the table and re-create the metadata
      // After that it's going to be just a normal append
      featureGroupApi.deleteContent(streamFeatureGroup);
    }

    SparkEngine.getInstance().writeOnlineDataframe(streamFeatureGroup, featureData,
        streamFeatureGroup.getOnlineTopicName(), utils.getKafkaConfig(streamFeatureGroup, writeOptions));
  }

  public <S> StreamFeatureGroup saveFeatureGroupMetaData(StreamFeatureGroup featureGroup, List<String> partitionKeys,
                                                     String hudiPrecombineKey, Map<String, String> writeOptions,
                                                     JobConfiguration sparkJobConfiguration, S featureData)
      throws FeatureStoreException, IOException {

    if (featureGroup.getFeatures() == null) {
      featureGroup.setFeatures(utils
          .parseFeatureGroupSchema(utils.convertToDefaultDataframe(featureData),
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

    // set write options for delta streamer job
    DeltaStreamerJobConf deltaStreamerJobConf = new DeltaStreamerJobConf();
    deltaStreamerJobConf.setWriteOptions(writeOptions != null ? writeOptions.entrySet().stream()
        .map(e -> new Option(e.getKey(), e.getValue()))
        .collect(Collectors.toList())
        : null);
    deltaStreamerJobConf.setSparkJobConfiguration(sparkJobConfiguration);

    featureGroup.setDeltaStreamerJobConf(deltaStreamerJobConf);

    // Send Hopsworks the request to create a new feature group
    StreamFeatureGroup apiFG = featureGroupApi.save(featureGroup);

    if (featureGroup.getVersion() == null) {
      LOGGER.info("VersionWarning: No version provided for creating feature group `" + featureGroup.getName()
          + "`, incremented version to `" + apiFG.getVersion() + "`.");
    }


    // Update the original object - Hopsworks returns the incremented version
    featureGroup.setId(apiFG.getId());
    featureGroup.setVersion(apiFG.getVersion());
    featureGroup.setLocation(apiFG.getLocation());
    featureGroup.setStatisticsConfig(apiFG.getStatisticsConfig());
    featureGroup.setOnlineTopicName(apiFG.getOnlineTopicName());

    /* if hudi precombine key was not provided and TimeTravelFormat is HUDI, retrieve from backend and set */
    if (hudiPrecombineKey == null) {
      List<Feature> features = apiFG.getFeatures();
      featureGroup.setFeatures(features);
    }

    return featureGroup;
  }
}
