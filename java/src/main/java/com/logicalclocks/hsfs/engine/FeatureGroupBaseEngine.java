/*
 * Copyright (c) 2020 Logical Clocks AB *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.ExternalFeatureGroup;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.TagsApi;

import java.io.IOException;
import java.util.ArrayList;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FeatureGroupBaseEngine {
  protected FeatureGroupApi featureGroupApi;
  protected TagsApi tagsApi;

  public FeatureGroupBaseEngine() {
    featureGroupApi = new FeatureGroupApi();
    tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);
  }

  // for testing
  public FeatureGroupBaseEngine(FeatureGroupApi featureGroupApi, TagsApi tagsApi) {
    this.featureGroupApi = featureGroupApi;
    this.tagsApi = tagsApi;
  }

  public void delete(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    featureGroupApi.delete(featureGroupBase);
  }

  public void addTag(FeatureGroupBase featureGroupBase, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureGroupBase, name, value);
  }

  public Object getTag(FeatureGroupBase featureGroupBase, String name) throws FeatureStoreException, IOException {
    return tagsApi.get(featureGroupBase, name);
  }

  public Map<String, Object> getTags(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    return tagsApi.get(featureGroupBase);
  }

  public void deleteTag(FeatureGroupBase featureGroupBase, String name)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureGroupBase, name);
  }

  public <T extends FeatureGroupBase> void updateDescription(FeatureGroupBase featureGroup, String description,
                                                             Class<T> fgClass)
      throws FeatureStoreException, IOException {
    featureGroup.setDescription(description);
    T apiFG = featureGroupApi.updateMetadata(featureGroup, "updateMetadata", fgClass);
    featureGroup.setDescription(apiFG.getDescription());
  }

  public <T extends FeatureGroupBase> void updateFeatures(FeatureGroupBase featureGroup, List<Feature> features,
                                                          Class<T> fgClass)
      throws FeatureStoreException, IOException {
    List<Feature> newFeatures = new ArrayList<>();
    for (Feature feature : featureGroup.getFeatures()) {
      Optional<Feature> match =
          features.stream().filter(updated -> updated.getName().equalsIgnoreCase(feature.getName())).findAny();
      if (!match.isPresent()) {
        newFeatures.add(feature);
      } else {
        match.get().setType(feature.getType());
        newFeatures.add(match.get());
      }
    }
    newFeatures.addAll(features);
    featureGroup.setFeatures(newFeatures);
    T apiFG = featureGroupApi.updateMetadata(featureGroup, "updateMetadata", fgClass);
    featureGroup.setFeatures(apiFG.getFeatures());
    featureGroup.unloadSubject();
  }

  public <T extends FeatureGroupBase> void appendFeatures(FeatureGroupBase featureGroup, List<Feature> features,
                                                          Class<T> fgClass)
      throws FeatureStoreException, IOException, ParseException {
    featureGroup.getFeatures().addAll(features);
    T apiFG = featureGroupApi.updateMetadata(featureGroup, "updateMetadata",
        fgClass);
    featureGroup.setFeatures(apiFG.getFeatures());
    featureGroup.unloadSubject();
    if (featureGroup instanceof FeatureGroup) {
      SparkEngine.getInstance().writeEmptyDataframe(featureGroup);
    }
  }

  public <T extends FeatureGroupBase> void updateStatisticsConfig(FeatureGroupBase featureGroup, Class<T> fgClass)
      throws FeatureStoreException, IOException {
    T apiFG = featureGroupApi.updateMetadata(featureGroup, "updateStatsConfig", fgClass);
    featureGroup.setStatisticsConfig(apiFG.getStatisticsConfig());
  }

  private FeatureGroupBase initFeatureGroupBase(FeatureGroupBase featureGroup) {
    if (featureGroup instanceof FeatureGroup) {
      return new FeatureGroup(featureGroup.getFeatureStore(), featureGroup.getId());
    } else if (featureGroup instanceof StreamFeatureGroup) {
      return new StreamFeatureGroup(featureGroup.getFeatureStore(), featureGroup.getId());
    } else if (featureGroup instanceof ExternalFeatureGroup) {
      return new ExternalFeatureGroup(featureGroup.getFeatureStore(), featureGroup.getId());
    }
    return new FeatureGroupBase();
  }
}
