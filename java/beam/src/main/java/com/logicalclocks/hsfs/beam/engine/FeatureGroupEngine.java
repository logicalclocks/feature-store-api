/*
 *  Copyright (c) 2023. Hopsworks AB
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

package com.logicalclocks.hsfs.beam.engine;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.JobConfiguration;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.beam.FeatureStore;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.FeatureGroupEngineBase;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FeatureGroupEngine extends FeatureGroupEngineBase {

  @SneakyThrows
  public BeamProducer insertStream(StreamFeatureGroup streamFeatureGroup, Map<String, String> writeOptions) {
    return BeamEngine.getInstance().insertStream(streamFeatureGroup, writeOptions);
  }

  public StreamFeatureGroup getStreamFeatureGroup(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    StreamFeatureGroup[] streamFeatureGroups =
      featureGroupApi.getInternal(featureStore, fgName, fgVersion, StreamFeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    StreamFeatureGroup resultFg = streamFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }

  public List<StreamFeatureGroup> getStreamFeatureGroups(FeatureStore featureStore, String fgName)
      throws FeatureStoreException, IOException {
    StreamFeatureGroup[] streamFeatureGroups =
      featureGroupApi.getInternal(featureStore, fgName, null, StreamFeatureGroup[].class);

    return Arrays.asList(streamFeatureGroups);
  }

  public void save(StreamFeatureGroup featureGroup, List<String> partitionKeys, String precombineKeyName,
                   Map<String, String> writeOptions, JobConfiguration materializationJobConfiguration)
      throws FeatureStoreException, IOException {
    if (featureGroup.getId() != null) {
      // Feature group metadata already exists. Just return
      return;
    }

    // verify primary, partition, event time and hudi precombine keys
    utils.verifyAttributeKeyNames(featureGroup, partitionKeys, precombineKeyName);

    StreamFeatureGroup apiFG = (StreamFeatureGroup) featureGroupApi.saveFeatureGroupMetaData(featureGroup,
        partitionKeys, precombineKeyName, writeOptions, materializationJobConfiguration, StreamFeatureGroup.class);
    featureGroup.setOnlineTopicName(apiFG.getOnlineTopicName());
  }

  public StreamFeatureGroup getOrCreateFeatureGroup(FeatureStore featureStore, @NonNull String name,
                                                    Integer version,
                                                    String description,
                                                    Boolean onlineEnabled,
                                                    TimeTravelFormat timeTravelFormat,
                                                    List<String> primaryKeys,
                                                    List<String> partitionKeys,
                                                    String eventTime,
                                                    String hudiPrecombineKey,
                                                    List<Feature> features,
                                                    StatisticsConfig statisticsConfig,
                                                    StorageConnector storageConnector,
                                                    String path)
      throws IOException, FeatureStoreException {

    try {
      return getStreamFeatureGroup(featureStore, name, version);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270009")) {
        return StreamFeatureGroup.builder()
            .featureStore(featureStore)
            .name(name)
            .version(version)
            .description(description)
            .onlineEnabled(onlineEnabled)
            .timeTravelFormat(timeTravelFormat)
            .primaryKeys(primaryKeys)
            .partitionKeys(partitionKeys)
            .eventTime(eventTime)
            .hudiPrecombineKey(hudiPrecombineKey)
            .features(features)
            .statisticsConfig(statisticsConfig)
            .storageConnector(storageConnector)
            .path(path)
            .build();
      } else {
        throw e;
      }
    }
  }
}
