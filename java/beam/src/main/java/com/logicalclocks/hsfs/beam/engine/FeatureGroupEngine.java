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

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.beam.FeatureStore;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.FeatureGroupEngineBase;
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
}
