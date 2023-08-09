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

package com.logicalclocks.hsfs.beam.constructor;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.constructor.QueryBase;

import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.Map;

public class Query extends QueryBase<Query, StreamFeatureGroup, PCollection<Object>> {
  @Override
  public String sql() {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public String sql(Storage storage) {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object read() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object read(boolean online) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void show(int numRows) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void show(boolean online, int numRows) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }
}
