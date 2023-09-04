package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.constructor.Query;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

  @Override
  public Query getBatchQuery(FeatureView featureView, Date date, Date date1, Boolean withLabels, Integer integer)
      throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public String getBatchQueryString(FeatureView featureView, Date startTime, Date endTime, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public FeatureView getOrCreateFeatureView(FeatureStore featureStore, String name, Integer version, Query query,
      String description, List<String> labels) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public List<Object> getBatchData(FeatureView featureView, Date startTime, Date endTime,
      Map<String, String> readOptions, Integer trainingDataVersion) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }
}
