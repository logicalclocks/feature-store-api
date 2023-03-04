/*
 *  Copyright (c) 2022. Hopsworks AB
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

package com.logicalclocks.hsfs.metadata;

import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.ExternalFeatureGroup;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.base.StatisticsConfig;
import com.logicalclocks.hsfs.StreamFeatureGroup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MetaDataUtils {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();

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

  public StreamFeatureGroup getOrCreateStreamFeatureGroup(FeatureStore featureStore, String name, Integer version,
                                                          String description, List<String> primaryKeys,
                                                          List<String> partitionKeys, String hudiPrecombineKey,
                                                          boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig,
                                                          String eventTime) throws IOException, FeatureStoreException {


    StreamFeatureGroup featureGroup;
    try {
      featureGroup = getStreamFeatureGroup(featureStore, name, version);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270009")) {
        featureGroup = StreamFeatureGroup.builder()
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

  public List<ExternalFeatureGroup> getExternalFeatureGroups(FeatureStore featureStore, String fgName)
      throws FeatureStoreException, IOException {
    ExternalFeatureGroup[] offlineFeatureGroups =
        featureGroupApi.getInternal(featureStore, fgName, null, ExternalFeatureGroup[].class);

    return Arrays.asList(offlineFeatureGroups);
  }

  public ExternalFeatureGroup getExternalFeatureGroup(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    ExternalFeatureGroup[] offlineFeatureGroups =
        featureGroupApi.getInternal(featureStore, fgName, fgVersion, ExternalFeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    ExternalFeatureGroup resultFg = offlineFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }
}
