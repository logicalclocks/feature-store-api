package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.constructor.Query;

import java.io.IOException;
import java.util.List;

public class FeatureViewEngine extends FeatureViewEngineBase<Query, FeatureView, FeatureStore, StreamFeatureGroup,
    List<Object>> {

  @Override
  public FeatureView update(FeatureView featureView) throws FeatureStoreException, IOException {
    featureViewApi.update(featureView, FeatureView.class);
    return featureView;
  }

  @Override
  public FeatureView get(FeatureStore featureStore, String name, Integer version)
      throws FeatureStoreException, IOException {
    FeatureView featureView = get(featureStore, name, version, FeatureView.class);
    featureView.setFeatureStore(featureStore);
    return featureView;
  }
}
