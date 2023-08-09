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

package com.logicalclocks.hsfs.flink.constructor;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.constructor.QueryBase;

import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.util.Map;

@NoArgsConstructor
public class Query extends QueryBase<Query, StreamFeatureGroup, DataStream<?>> {

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
