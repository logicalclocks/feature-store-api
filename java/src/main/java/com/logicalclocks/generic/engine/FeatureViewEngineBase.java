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

package com.logicalclocks.generic.engine;

import com.google.common.collect.Lists;
import com.logicalclocks.generic.constructor.Query;
import com.logicalclocks.generic.metadata.FeatureViewApi;
import com.logicalclocks.generic.metadata.TagsApi;
import com.logicalclocks.generic.EntityEndpointType;
import com.logicalclocks.generic.FeatureStoreBase;
import com.logicalclocks.generic.FeatureStoreException;
import com.logicalclocks.generic.FeatureViewBase;
import com.logicalclocks.generic.TrainingDatasetFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class FeatureViewEngineBase {

  private FeatureViewApi featureViewApi = new FeatureViewApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_VIEW);
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureViewEngineBase.class);

  public FeatureViewBase save(FeatureViewBase featureViewBase) throws FeatureStoreException, IOException {
    featureViewBase.setFeatures(makeLabelFeatures(featureViewBase.getLabels()));
    FeatureViewBase updatedFeatureViewBase = featureViewApi.save(featureViewBase);
    featureViewBase.setVersion(updatedFeatureViewBase.getVersion());
    featureViewBase.setFeatures(updatedFeatureViewBase.getFeatures());
    return featureViewBase;
  }

  private List<TrainingDatasetFeature> makeLabelFeatures(List<String> labels) {
    if (labels == null || labels.isEmpty()) {
      return Lists.newArrayList();
    } else {
      return labels.stream().map(label -> new TrainingDatasetFeature(label.toLowerCase(), true))
          .collect(Collectors.toList());
    }
  }

  public FeatureViewBase update(FeatureViewBase featureViewBase) throws FeatureStoreException,
      IOException {
    FeatureViewBase featureViewBaseUpdated = featureViewApi.update(featureViewBase);
    featureViewBase.setDescription(featureViewBaseUpdated.getDescription());
    return featureViewBase;
  }

  public FeatureViewBase get(FeatureStoreBase featureStoreBase, String name, Integer version)
      throws FeatureStoreException, IOException {
    FeatureViewBase featureViewBase = featureViewApi.get(featureStoreBase, name, version);
    featureViewBase.setFeatureStoreBase(featureStoreBase);
    featureViewBase.getFeatures().stream()
        .filter(f -> f.getFeatureGroup() != null)
        .forEach(f -> f.getFeatureGroup().setFeatureStoreBase(featureStoreBase));
    featureViewBase.getQuery().getLeftFeatureGroup().setFeatureStoreBase(featureStoreBase);
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
      fv.setFeatureStoreBase(featureStoreBase);
      fv.getFeatures().stream()
          .filter(f -> f.getFeatureGroup() != null)
          .forEach(f -> f.getFeatureGroup().setFeatureStoreBase(featureStoreBase));
      fv.getQuery().getLeftFeatureGroup().setFeatureStoreBase(featureStoreBase);
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

  public void createTrainingDataset() {
  }

  public void writeTrainingDataset() {
  }

  public void getTrainingDataset() {
  }

  private void setTrainSplit() {
  }

  private void createTrainingDataMetadata(){
  }

  private void setEventTime() {
  }

  private Date getStartTime() {
    return new Date(1000);
  }

  private Date getEndTime() {
    return new Date();
  }

  private void getTrainingDataMetadata() {

  }

  public void computeStatistics() {
  }

  private void convertSplitDatasetsToMap() {
  }

  public void recreateTrainingDataset() {
  }

  private void readDataset() {
  }

  public void deleteTrainingData() {
  }

  public void deleteTrainingDatasetOnly() {
  }

  public String getBatchQueryString(FeatureViewBase featureViewBase, Date startTime, Date endTime,
                                    Integer trainingDataVersion) throws FeatureStoreException, IOException {
    Query query = getBatchQuery(featureViewBase, startTime, endTime, false, trainingDataVersion);
    return query.sql();
  }

  public Query getBatchQuery(FeatureViewBase featureViewBase, Date startTime, Date endTime, Boolean withLabels,
                             Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    Query query = featureViewApi.getBatchQuery(
        featureViewBase.getFeatureStoreBase(),
        featureViewBase.getName(),
        featureViewBase.getVersion(),
        startTime == null ? null : startTime.getTime(),
        endTime == null ? null : endTime.getTime(),
        withLabels,
        trainingDataVersion
    );
    query.getLeftFeatureGroup().setFeatureStoreBase(
        featureViewBase.getQuery().getLeftFeatureGroup().getFeatureStoreBase());
    return query;
  }

  public void getBatchData() throws FeatureStoreException, IOException {
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
