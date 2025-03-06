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

package com.logicalclocks.hsfs.beam;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.beam.constructor.Query;
import com.logicalclocks.hsfs.beam.engine.FeatureViewEngine;
import lombok.NonNull;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class FeatureView extends FeatureViewBase<FeatureView, FeatureStore, Query, PCollection<Object>> {

  private static final FeatureViewEngine featureViewEngine = new FeatureViewEngine();

  public static class FeatureViewBuilder {

    private String name;
    private Integer version;
    private String description;
    private FeatureStore featureStore;
    private Query query;
    private List<String> labels;

    public FeatureViewBuilder(FeatureStore featureStore) {
      this.featureStore = featureStore;
    }

    public FeatureView.FeatureViewBuilder name(String name) {
      this.name = name;
      return this;
    }

    public FeatureView.FeatureViewBuilder version(Integer version) {
      this.version = version;
      return this;
    }

    public FeatureView.FeatureViewBuilder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * Query of a feature view. Note that `as_of` argument in the `Query` will be ignored because feature view does
     * not support time travel query.
     *
     * @param query Query object
     * @return builder
     */
    public FeatureView.FeatureViewBuilder query(Query query) {
      this.query = query;
      if (query.isTimeTravel()) {
        FeatureViewBase.LOGGER.info("`as_of` argument in the `Query` will be ignored because "
            + "feature view does not support time travel query.");
      }
      return this;
    }

    public FeatureView.FeatureViewBuilder labels(List<String> labels) {
      this.labels = labels;
      return this;
    }

    public FeatureView build() throws FeatureStoreException, IOException {
      FeatureView
          featureView = new FeatureView(name, version, query, description, featureStore, labels);
      featureViewEngine.save(featureView, FeatureView.class);
      return featureView;
    }
  }

  public FeatureView(@NonNull String name, Integer version, @NonNull Query query, String description,
                     @NonNull FeatureStore featureStore, List<String> labels) {
    this.name = name;
    this.version = version;
    this.query = query;
    this.description = description;
    this.featureStore = featureStore;
    this.labels = labels != null ? labels.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
  }
}
