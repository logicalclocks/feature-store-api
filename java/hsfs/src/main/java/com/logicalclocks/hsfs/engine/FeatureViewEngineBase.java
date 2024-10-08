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

package com.logicalclocks.hsfs.engine;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.constructor.Join;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.metadata.FeatureViewApi;
import com.logicalclocks.hsfs.metadata.TagsApi;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.TrainingDatasetBase;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class FeatureViewEngineBase<T1 extends QueryBase<T1, T4, T5>, T2
    extends FeatureViewBase<T2, T3, T1, T5>, T3 extends FeatureStoreBase<T1>, T4 extends FeatureGroupBase, T5> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(FeatureViewEngineBase.class);

  protected FeatureViewApi featureViewApi = new FeatureViewApi();
  protected TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_VIEW);

  public static String AMBIGUOUS_LABEL_ERROR = "Provided label '%s' is ambiguous and exists in more than one feature "
      + "groups. You can provide the label with the prefix you specify in the join.";
  public static String LABEL_NOT_EXIST_ERROR = "Provided label '%s' do not exist in any of the feature groups.";

  public T2 save(T2 featureViewBase, Class<T2> fvType) throws FeatureStoreException, IOException {
    featureViewBase.setFeatures(makeLabelFeatures(featureViewBase.getQuery(), featureViewBase.getLabels()));
    T2 updatedFeatureViewBase = featureViewApi.save(featureViewBase, fvType);
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
      for (Feature feat : (List<Feature>) query.getLeftFeatures()) {
        labelWithPrefixToFeature.put(feat.getName(), feat.getName());
        labelWithPrefixToFeatureGroup.put(feat.getName(),
            (new StreamFeatureGroup(null, feat.getFeatureGroupId())));
      }
      for (Join join : (List<Join>) query.getJoins()) {
        for (Feature feat : (List<Feature>) join.getQuery().getLeftFeatures()) {
          String labelWithPrefix = join.getPrefix() + feat.getName();
          labelWithPrefixToFeature.put(labelWithPrefix, feat.getName());
          labelWithPrefixToFeatureGroup.put(labelWithPrefix,
              new StreamFeatureGroup(null, feat.getFeatureGroupId()));
          List<FeatureGroupBase> featureGroups = labelToFeatureGroups.getOrDefault(feat.getName(),
              Lists.newArrayList());
          featureGroups.add(new StreamFeatureGroup(null, feat.getFeatureGroupId()));
          labelToFeatureGroups.put(feat.getName(), featureGroups);
        }
      }
      List<TrainingDatasetFeature> trainingDatasetFeatures = Lists.newArrayList();
      for (String label : labels) {
        if (labelWithPrefixToFeature.containsKey(label)) {
          trainingDatasetFeatures.add(new TrainingDatasetFeature(
                (StreamFeatureGroup) labelWithPrefixToFeatureGroup.get(label.toLowerCase()),
                  labelWithPrefixToFeature.get(label.toLowerCase()),
                  true));
        } else if (labelToFeatureGroups.containsKey(label)) {
          if (labelToFeatureGroups.get(label.toLowerCase()).size() > 1) {
            throw new FeatureStoreException(String.format(AMBIGUOUS_LABEL_ERROR, label));
          }
          trainingDatasetFeatures.add(new TrainingDatasetFeature(
                (StreamFeatureGroup) labelToFeatureGroups.get(label.toLowerCase()).get(0),
                  label.toLowerCase(),
                  true));
        } else {
          throw new FeatureStoreException(String.format(LABEL_NOT_EXIST_ERROR, label));
        }

      }
      return trainingDatasetFeatures;
    }
  }

  public abstract T2 update(T2 featureView) throws FeatureStoreException, IOException;

  public abstract T2 get(T3 featureStore, String name, Integer version) throws FeatureStoreException, IOException;

  public T2 get(T3 featureStoreBase, String name, Integer version, Class<T2> fvType)
      throws FeatureStoreException, IOException {
    FeatureViewBase featureViewBase = featureViewApi.get(featureStoreBase, name, version, fvType);
    featureViewBase.setFeatureStore(featureStoreBase);
    List<TrainingDatasetFeature> features = featureViewBase.getFeatures();
    features.stream()
          .filter(f -> f.getFeaturegroup() != null)
          .forEach(f -> f.getFeaturegroup().setFeatureStore(featureStoreBase));
    featureViewBase.getQuery().getLeftFeatureGroup().setFeatureStore(featureStoreBase);
    featureViewBase.setLabels(
        features.stream()
          .filter(TrainingDatasetFeature::getLabel)
          .map(TrainingDatasetFeature::getName)
          .collect(Collectors.toList()));
    return (T2) featureViewBase;
  }

  public List<FeatureViewBase> get(T3 featureStoreBase, String name) throws FeatureStoreException,
      IOException {
    List<FeatureViewBase> featureViewBases = featureViewApi.get(featureStoreBase, name);
    for (FeatureViewBase fv : featureViewBases) {
      fv.setFeatureStore(featureStoreBase);
      List<TrainingDatasetFeature> features = fv.getFeatures();
      features.stream()
          .filter(f -> f.getFeaturegroup() != null)
          .forEach(f -> f.getFeaturegroup().setFeatureStore(featureStoreBase));
      fv.getQuery().getLeftFeatureGroup().setFeatureStore(featureStoreBase);
      fv.setLabels(
          features.stream()
              .filter(TrainingDatasetFeature::getLabel)
              .map(TrainingDatasetFeature::getName)
              .collect(Collectors.toList()));
    }
    return featureViewBases;
  }

  public void delete(T3 featureStoreBase, String name) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStoreBase, name);
  }

  public void delete(T3 featureStoreBase, String name, Integer version) throws FeatureStoreException,
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

  public T1 getBatchQuery(T2 featureView, Date startTime,  Date endTime, Boolean withLabels,
                          Integer trainingDataVersion, Class<T1> queryType) throws FeatureStoreException, IOException {
    QueryBase query;
    try {
      query = featureViewApi.getBatchQuery(
          featureView.getFeatureStore(),
          featureView.getName(),
          featureView.getVersion(),
          startTime == null ? null : startTime.getTime(),
          endTime == null ? null : endTime.getTime(),
          withLabels,
          trainingDataVersion,
          queryType
      );
    } catch (IOException e) {
      if (e.getMessage().contains("\"errorCode\":270172")) {
        throw new FeatureStoreException(
            "Cannot generate dataset or query from the given start/end time because"
                + " event time column is not available in the left feature groups."
                + " A start/end time should not be provided as parameters."
        );
      } else {
        throw e;
      }
    }
    query.getLeftFeatureGroup().setFeatureStore(featureView.getQuery().getLeftFeatureGroup().getFeatureStore());
    return (T1)query;
  }

  protected void setEventTime(FeatureViewBase featureView, TrainingDatasetBase trainingDataset) {
    String eventTime = featureView.getQuery().getLeftFeatureGroup().getEventTime();
    if (!Strings.isNullOrEmpty(eventTime)) {
      if (trainingDataset.getSplits() != null && !trainingDataset.getSplits().isEmpty()) {
        for (Split split : trainingDataset.getSplits()) {
          if (split.getSplitType() == Split.SplitType.TIME_SERIES_SPLIT
              && split.getName().equals(Split.TRAIN)
              && split.getStartTime() == null) {
            split.setStartTime(getStartTime());
          }
          if (split.getSplitType() == Split.SplitType.TIME_SERIES_SPLIT
              && split.getName().equals(Split.TEST)
              && split.getEndTime() == null) {
            split.setEndTime(getEndTime());
          }
        }
      } else {
        if (trainingDataset.getEventStartTime() == null) {
          trainingDataset.setEventStartTime(getStartTime());
        }
        if (trainingDataset.getEventEndTime() == null) {
          trainingDataset.setEventEndTime(getEndTime());
        }
      }
    }
  }

  public void deleteTrainingData(T2 featureView, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    featureViewApi.deleteTrainingData(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion(), trainingDataVersion);
  }

  public void deleteTrainingData(T2 featureView) throws FeatureStoreException, IOException {
    featureViewApi.deleteTrainingData(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion());
  }

  public void deleteTrainingDatasetOnly(T2 featureView, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    featureViewApi.deleteTrainingDatasetOnly(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion(), trainingDataVersion);
  }

  public void deleteTrainingDatasetOnly(T2 featureView) throws FeatureStoreException, IOException {
    featureViewApi.deleteTrainingDatasetOnly(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion());
  }
}
