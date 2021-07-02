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
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.OnDemandFeatureGroup;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.TagsApi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureGroupBaseEngine {
  protected FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  protected TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_GROUP);

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

  public void updateDescription(FeatureGroupBase featureGroup, String description)
      throws FeatureStoreException, IOException {
    FeatureGroupBase fgBaseSend = initFeatureGroupBase(featureGroup);
    fgBaseSend.setDescription(description);
    FeatureGroupBase apiFG = featureGroupApi.updateMetadata(fgBaseSend, "updateMetadata");
    featureGroup.setDescription(apiFG.getDescription());
  }

  public void appendFeatures(FeatureGroupBase featureGroup, List<Feature> features)
      throws FeatureStoreException, IOException, ParseException {
    Dataset<Row> emptyDataframe = SparkEngine.getInstance().getEmptyAppendedDataframe(featureGroup.read(), features);
    FeatureGroupBase fgBaseSend = initFeatureGroupBase(featureGroup);
    features.addAll(featureGroup.getFeatures());
    fgBaseSend.setFeatures(features);
    FeatureGroupBase apiFG = featureGroupApi.updateMetadata(fgBaseSend, "updateMetadata");
    featureGroup.setFeatures(apiFG.getFeatures());
    if (featureGroup instanceof FeatureGroup) {
      SparkEngine.getInstance().writeOfflineDataframe((FeatureGroup) featureGroup, emptyDataframe,
          HudiOperationType.UPSERT, new HashMap<>(), null);
    }
  }

  public void updateStatisticsConfig(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    FeatureGroupBase apiFG = featureGroupApi.updateMetadata(featureGroup, "updateStatsConfig");
    featureGroup.getStatisticsConfig().setCorrelations(apiFG.getStatisticsConfig().getCorrelations());
    featureGroup.getStatisticsConfig().setHistograms(apiFG.getStatisticsConfig().getHistograms());
  }

  private FeatureGroupBase initFeatureGroupBase(FeatureGroupBase featureGroup) {
    if (featureGroup instanceof FeatureGroup) {
      return new FeatureGroup(featureGroup.getFeatureStore(), featureGroup.getId());
    } else if (featureGroup instanceof OnDemandFeatureGroup) {
      return new OnDemandFeatureGroup(featureGroup.getFeatureStore(), featureGroup.getId());
    }
    return new FeatureGroupBase();
  }
}
