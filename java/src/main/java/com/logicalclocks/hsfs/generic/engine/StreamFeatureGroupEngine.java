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

package com.logicalclocks.hsfs.generic.engine;

import com.logicalclocks.hsfs.generic.DeltaStreamerJobConf;
import com.logicalclocks.hsfs.generic.Feature;
import com.logicalclocks.hsfs.generic.FeatureStoreException;
import com.logicalclocks.hsfs.generic.JobConfiguration;
import com.logicalclocks.hsfs.generic.StreamFeatureGroup;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.generic.metadata.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamFeatureGroupEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamFeatureGroupEngine.class);

  /*
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(FeatureStore featureStore, String name, Integer version,
                                                          String description, List<String> primaryKeys,
                                                          List<String> partitionKeys, String hudiPrecombineKey,
                                                          boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig,
                                                          String eventTime) throws IOException, FeatureStoreException {


    StreamFeatureGroup featureGroup;
    try {
      featureGroup =  featureGroupApi.getStreamFeatureGroup(featureStore, name, version);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270009")) {
        featureGroup =  StreamFeatureGroup.builder()
            .featureStore(featureStore)
            .name(name)
            .version(version)
            .description(description)
            .primaryKeys(primaryKeys)
            .partitionKeys(partitionKeys)
            .hudiPrecombineKey(hudiPrecombineKey)
            .onlineEnabled(onlineEnabled)
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
  */

  public StreamFeatureGroup saveFeatureGroupMetaData(StreamFeatureGroup featureGroup, List<String> partitionKeys,
                                                     String hudiPrecombineKey, Map<String, String> writeOptions,
                                                     JobConfiguration sparkJobConfiguration)
      throws FeatureStoreException, IOException {

    /* TODO (davit): this must be implemented in engine as parseFeatureGroupSchema is different in frameworks
    if (featureGroup.getFeatures() == null) {
      featureGroup.setFeatures(utils
          .parseFeatureGroupSchema(utils.sanitizeFeatureNames(featureData),
            featureGroup.getTimeTravelFormat()));
    }
     */

    LOGGER.info("Featuregroup features: " + featureGroup.getFeatures());

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
