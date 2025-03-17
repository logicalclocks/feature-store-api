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
import com.logicalclocks.hsfs.beam.FeatureView;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.beam.constructor.Query;
import com.logicalclocks.hsfs.engine.FeatureViewEngineBase;
import org.apache.beam.sdk.values.PCollection;
import java.io.IOException;
import java.util.List;

public class FeatureViewEngine extends FeatureViewEngineBase<Query, FeatureView, FeatureStore, StreamFeatureGroup,
    PCollection<Object>> {

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

  public FeatureView getOrCreateFeatureView(FeatureStore featureStore, String name, Integer version, Query query,
                                            String description, List<String> labels)
      throws FeatureStoreException, IOException {
    FeatureView featureView;
    try {
      featureView = get(featureStore, name, version, FeatureView.class);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270181")) {
        featureView = new FeatureView.FeatureViewBuilder(featureStore)
            .name(name)
            .version(version)
            .query(query)
            .description(description)
            .labels(labels)
            .build();
      } else {
        throw e;
      }
    }
    return featureView;
  }
}
