/*
 *  Copyright (c) 2020-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.TagsApi;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FeatureGroupEngineBase {

  protected FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  protected FeatureGroupUtils utils = new FeatureGroupUtils();
  protected TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);

  protected static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngineBase.class);

  public FeatureGroupEngineBase() {
  }

  // for testing
  public FeatureGroupEngineBase(FeatureGroupApi featureGroupApi, TagsApi tagsApi) {
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

  public <T extends FeatureGroupBase> void updateNotificationTopicName(FeatureGroupBase featureGroup,
          String notificationTopicName, Class<T> fgClass)
      throws FeatureStoreException, IOException {
    featureGroup.setNotificationTopicName(notificationTopicName);
    T apiFG = featureGroupApi.updateMetadata(featureGroup, "updateMetadata", fgClass);
    featureGroup.setNotificationTopicName(apiFG.getNotificationTopicName());
  }

  public <T extends FeatureGroupBase> void updateDeprecated(FeatureGroupBase featureGroup, Boolean deprecate,
                                                            Class<T> fgClass)
      throws FeatureStoreException, IOException {
    T apiFG = featureGroupApi.updateMetadata(featureGroup, "deprecate", deprecate, fgClass);
    featureGroup.setDeprecated(apiFG.getDeprecated());
  }

  public <T extends FeatureGroupBase> void updateFeatures(FeatureGroupBase featureGroup, List<Feature> features,
                                                          Class<T> fgClass)
      throws FeatureStoreException, IOException {
    List<Feature> newFeatures = new ArrayList<>();
    for (Feature feature : (List<Feature>) featureGroup.getFeatures()) {
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

  public <T extends FeatureGroupBase> void updateStatisticsConfig(FeatureGroupBase featureGroup, Class<T> fgClass)
      throws FeatureStoreException, IOException {
    T apiFG = featureGroupApi.updateMetadata(featureGroup, "updateStatsConfig", fgClass);
    featureGroup.setStatisticsConfig(apiFG.getStatisticsConfig());
  }

  protected  <T extends FeatureGroupBase> T saveExtennalFeatureGroupMetaData(T externalFeatureGroup,
                                                                             Class<T> fgClass)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    return (T) featureGroupApi.saveInternal(externalFeatureGroup,
        hopsworksClient.buildStringEntity(externalFeatureGroup),
        fgClass);
  }
}
