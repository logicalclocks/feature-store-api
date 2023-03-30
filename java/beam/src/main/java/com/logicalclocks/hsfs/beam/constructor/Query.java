package com.logicalclocks.hsfs.beam.constructor;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.constructor.QueryBase;
import org.apache.beam.sdk.Pipeline;

import java.io.IOException;
import java.util.Map;

public class Query extends QueryBase<Query, StreamFeatureGroup, Pipeline> {
  @Override
  public String sql() {
    return null;
  }

  @Override
  public String sql(Storage storage) {
    return null;
  }

  @Override
  public Object read() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object read(boolean b) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object read(boolean b, Map<String, String> map) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public void show(int i) throws FeatureStoreException, IOException {

  }

  @Override
  public void show(boolean b, int i) throws FeatureStoreException, IOException {

  }
}
