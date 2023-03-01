/*
 *  Copyright (c) 2022. Hopsworks AB
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

package com.logicalclocks.base.engine;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.base.Feature;
import com.logicalclocks.base.constructor.Join;
import com.logicalclocks.base.constructor.QueryBase;
import com.logicalclocks.base.FeatureGroupBase;
import com.logicalclocks.base.metadata.FeatureViewApi;
import com.logicalclocks.base.metadata.TagsApi;
import com.logicalclocks.base.EntityEndpointType;
import com.logicalclocks.base.FeatureStoreBase;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.FeatureViewBase;
import com.logicalclocks.base.TrainingDatasetFeature;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class FeatureViewEngineBase {

  protected FeatureViewApi featureViewApi = new FeatureViewApi();
  protected TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_VIEW);
  public static String AMBIGUOUS_LABEL_ERROR = "Provided label '%s' is ambiguous and exists in more than one feature "
      + "groups. You can provide the label with the prefix you specify in the join.";
  public static String LABEL_NOT_EXIST_ERROR = "Provided label '%s' do not exist in any of the feature groups.";

  public FeatureViewBase save(FeatureViewBase featureViewBase) throws FeatureStoreException, IOException {
    featureViewBase.setFeatures(makeLabelFeatures(featureViewBase.getQuery(), featureViewBase.getLabels()));
    FeatureViewBase updatedFeatureViewBase = featureViewApi.save(featureViewBase, FeatureViewBase.class);
    featureViewBase.setVersion(updatedFeatureViewBase.getVersion());
    featureViewBase.setFeatures(updatedFeatureViewBase.getFeatures());
    return featureViewBase;
  }

  public static List<TrainingDatasetFeature> makeLabelFeatures(QueryBase query, List<String> labels)
      throws FeatureStoreException {
    if (labels == null || labels.isEmpty()) {
      return Lists.newArrayList();
    } else {
      // If provided label matches column with prefix, then attach label.
      // If provided label matches only one column without prefix, then attach label. (For
      // backward compatibility purpose, as of v3.0, labels are matched to columns without prefix.)
      // If provided label matches multiple columns without prefix, then raise exception because it is ambiguous.

      Map<String, String> labelWithPrefixToFeature = Maps.newHashMap();
      Map<String, FeatureGroupBase> labelWithPrefixToFeatureGroup = Maps.newHashMap();
      Map<String, List<FeatureGroupBase>> labelToFeatureGroups = Maps.newHashMap();
      for (Feature feat : query.getLeftFeatures()) {
        labelWithPrefixToFeature.put(feat.getName(), feat.getName());
        labelWithPrefixToFeatureGroup.put(feat.getName(),
            (new FeatureGroupBase(null, feat.getFeatureGroupId())));
      }
      for (Join join : query.getJoins()) {
        for (Feature feat : join.getQuery().getLeftFeatures()) {
          String labelWithPrefix = join.getPrefix() + feat.getName();
          labelWithPrefixToFeature.put(labelWithPrefix, feat.getName());
          labelWithPrefixToFeatureGroup.put(labelWithPrefix,
              new FeatureGroupBase(null, feat.getFeatureGroupId()));
          List<FeatureGroupBase> featureGroups = labelToFeatureGroups.getOrDefault(feat.getName(),
              Lists.newArrayList());
          featureGroups.add(new FeatureGroupBase(null, feat.getFeatureGroupId()));
          labelToFeatureGroups.put(feat.getName(), featureGroups);
        }
      }
      List<TrainingDatasetFeature> trainingDatasetFeatures = Lists.newArrayList();
      for (String label : labels) {
        if (labelWithPrefixToFeature.containsKey(label)) {
          trainingDatasetFeatures.add(new TrainingDatasetFeature(
              labelWithPrefixToFeatureGroup.get(label.toLowerCase()),
              labelWithPrefixToFeature.get(label.toLowerCase()),
              true));
        } else if (labelToFeatureGroups.containsKey(label)) {
          if (labelToFeatureGroups.get(label.toLowerCase()).size() > 1) {
            throw new FeatureStoreException(String.format(AMBIGUOUS_LABEL_ERROR, label));
          }
          trainingDatasetFeatures.add(new TrainingDatasetFeature(
              labelToFeatureGroups.get(label.toLowerCase()).get(0),
              label.toLowerCase(),
              true));
        } else {
          throw new FeatureStoreException(String.format(LABEL_NOT_EXIST_ERROR, label));
        }

      }
      return trainingDatasetFeatures;
    }
  }

  public <T extends FeatureViewBase> FeatureViewBase get(FeatureStoreBase featureStoreBase, String name,
                                                          Integer version, Class<T> fvType)
      throws FeatureStoreException, IOException {
    FeatureViewBase featureViewBase = featureViewApi.get(featureStoreBase, name, version, fvType);
    featureViewBase.setFeatureStore(featureStoreBase);
    featureViewBase.getFeatures().stream()
        .filter(f -> f.getFeatureGroup() != null)
        .forEach(f -> f.getFeatureGroup().setFeatureStore(featureStoreBase));
    featureViewBase.getQuery().getLeftFeatureGroup().setFeatureStore(featureStoreBase);
    featureViewBase.setLabels(
        featureViewBase.getFeatures().stream()
            .filter(TrainingDatasetFeature::getLabel)
            .map(TrainingDatasetFeature::getName)
            .collect(Collectors.toList()));
    return featureViewBase;
  }

  public List<FeatureViewBase> get(FeatureStoreBase featureStoreBase, String name) throws FeatureStoreException,
      IOException {
    List<FeatureViewBase> featureViewBases = featureViewApi.get(featureStoreBase, name);
    for (FeatureViewBase fv : featureViewBases) {
      fv.setFeatureStore(featureStoreBase);
      fv.getFeatures().stream()
          .filter(f -> f.getFeatureGroup() != null)
          .forEach(f -> f.getFeatureGroup().setFeatureStore(featureStoreBase));
      fv.getQuery().getLeftFeatureGroup().setFeatureStore(featureStoreBase);
      fv.setLabels(
          fv.getFeatures().stream()
              .filter(TrainingDatasetFeature::getLabel)
              .map(TrainingDatasetFeature::getName)
              .collect(Collectors.toList()));
    }
    return featureViewBases;
  }

  public void delete(FeatureStoreBase featureStoreBase, String name) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStoreBase, name);
  }

  public void delete(FeatureStoreBase featureStoreBase, String name, Integer version) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStoreBase, name, version);
  }

  protected Date getStartTime() {
    return new Date(1000);
  }

  protected Date getEndTime() {
    return new Date();
  }

  public void addTag(FeatureViewBase featureViewBase, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureViewBase, name, value);
  }

  public void addTag(FeatureViewBase featureViewBase, String name, Object value, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureViewBase, trainingDataVersion, name, value);
  }

  public void deleteTag(FeatureViewBase featureViewBase, String name)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureViewBase, name);
  }

  public void deleteTag(FeatureViewBase featureViewBase, String name, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureViewBase, trainingDataVersion, name);
  }

  public Object getTag(FeatureViewBase featureViewBase, String name)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureViewBase, name);
  }

  public Object getTag(FeatureViewBase featureViewBase, String name, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureViewBase, trainingDataVersion, name);
  }

  public Map<String, Object> getTags(FeatureViewBase featureViewBase)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureViewBase);
  }

  public Map<String, Object> getTags(FeatureViewBase featureViewBase, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureViewBase, trainingDataVersion);
  }
}
