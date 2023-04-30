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

package com.logicalclocks.hsfs.flink.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.engine.FeatureViewEngineBase;

import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import com.logicalclocks.hsfs.flink.constructor.Query;
import com.logicalclocks.hsfs.flink.FeatureView;
import com.logicalclocks.hsfs.flink.FeatureStore;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class FeatureViewEngine extends FeatureViewEngineBase<Query, FeatureView, FeatureStore, StreamFeatureGroup,
    DataStream<?>> {

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
  public DataStream getBatchData(FeatureView featureView, Date startTime, Date endTime, Map<String, String> readOptions,
      Integer trainingDataVersion) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }
}
