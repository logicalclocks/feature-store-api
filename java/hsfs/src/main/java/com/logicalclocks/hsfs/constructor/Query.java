package com.logicalclocks.hsfs.constructor;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.StreamFeatureGroup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Query extends QueryBase<Query, StreamFeatureGroup, List<Object>> {
  @Override
  public String sql() {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public String sql(Storage storage) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object read() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object read(boolean online) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void show(int i) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void show(boolean online, int numRows) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }
}
