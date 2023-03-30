package com.logicalclocks.hsfs.beam.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.beam.FeatureStore;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.FeatureGroupEngineBase;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FeatureGroupEngine extends FeatureGroupEngineBase {

  @SneakyThrows
  public HsfsBeamProducer insertStream(StreamFeatureGroup streamFeatureGroup) {
    return BeamEngine.getInstance().insertStream(streamFeatureGroup);
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
