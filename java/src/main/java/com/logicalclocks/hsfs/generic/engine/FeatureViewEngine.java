package com.logicalclocks.hsfs.generic.engine;

import com.google.common.collect.Lists;
import com.logicalclocks.hsfs.generic.EntityEndpointType;
import com.logicalclocks.hsfs.generic.FeatureStore;
import com.logicalclocks.hsfs.generic.FeatureStoreException;
import com.logicalclocks.hsfs.generic.FeatureView;
import com.logicalclocks.hsfs.generic.TrainingDatasetFeature;
import com.logicalclocks.hsfs.generic.constructor.Query;
import com.logicalclocks.hsfs.generic.metadata.FeatureViewApi;
import com.logicalclocks.hsfs.generic.metadata.TagsApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureViewEngine {

  private FeatureViewApi featureViewApi = new FeatureViewApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_VIEW);
  //private TrainingDatasetEngine trainingDatasetEngine = new TrainingDatasetEngine();
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureViewEngine.class);
  //private StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.TRAINING_DATASET);

  public FeatureView save(FeatureView featureView) throws FeatureStoreException, IOException {
    featureView.setFeatures(makeLabelFeatures(featureView.getLabels()));
    FeatureView updatedFeatureView = featureViewApi.save(featureView);
    featureView.setVersion(updatedFeatureView.getVersion());
    featureView.setFeatures(updatedFeatureView.getFeatures());
    return featureView;
  }

  private List<TrainingDatasetFeature> makeLabelFeatures(List<String> labels) {
    if (labels == null || labels.isEmpty()) {
      return Lists.newArrayList();
    } else {
      return labels.stream().map(label -> new TrainingDatasetFeature(label.toLowerCase(), true))
          .collect(Collectors.toList());
    }
  }

  public FeatureView update(FeatureView featureView) throws FeatureStoreException,
      IOException {
    FeatureView featureViewUpdated = featureViewApi.update(featureView);
    featureView.setDescription(featureViewUpdated.getDescription());
    return featureView;
  }

  public FeatureView get(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    FeatureView featureView = featureViewApi.get(featureStore, name, version);
    featureView.setFeatureStore(featureStore);
    featureView.getFeatures().stream()
        .filter(f -> f.getFeatureGroup() != null)
        .forEach(f -> f.getFeatureGroup().setFeatureStore(featureStore));
    featureView.getQuery().getLeftFeatureGroup().setFeatureStore(featureStore);
    featureView.setLabels(
        featureView.getFeatures().stream()
            .filter(TrainingDatasetFeature::getLabel)
            .map(TrainingDatasetFeature::getName)
            .collect(Collectors.toList()));
    return featureView;
  }

  public List<FeatureView> get(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    List<FeatureView> featureViews = featureViewApi.get(featureStore, name);
    for (FeatureView fv : featureViews) {
      fv.setFeatureStore(featureStore);
      fv.getFeatures().stream()
          .filter(f -> f.getFeatureGroup() != null)
          .forEach(f -> f.getFeatureGroup().setFeatureStore(featureStore));
      fv.getQuery().getLeftFeatureGroup().setFeatureStore(featureStore);
      fv.setLabels(
          fv.getFeatures().stream()
              .filter(TrainingDatasetFeature::getLabel)
              .map(TrainingDatasetFeature::getName)
              .collect(Collectors.toList()));
    }
    return featureViews;
  }

  public void delete(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStore, name);
  }

  public void delete(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStore, name, version);
  }

  // TODO (davit): this methods must move to spark
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

  public String getBatchQueryString(FeatureView featureView, Date startTime, Date endTime, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    Query query = getBatchQuery(featureView, startTime, endTime, false, trainingDataVersion);
    return query.sql();
  }

  public Query getBatchQuery(FeatureView featureView, Date startTime, Date endTime, Boolean withLabels,
      Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    Query query = featureViewApi.getBatchQuery(
        featureView.getFeatureStore(),
        featureView.getName(),
        featureView.getVersion(),
        startTime == null ? null : startTime.getTime(),
        endTime == null ? null : endTime.getTime(),
        withLabels,
        trainingDataVersion
    );
    query.getLeftFeatureGroup().setFeatureStore(featureView.getQuery().getLeftFeatureGroup().getFeatureStore());
    return query;
  }

  public void getBatchData() throws FeatureStoreException, IOException {
  }

  public void addTag(FeatureView featureView, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureView, name, value);
  }

  public void addTag(FeatureView featureView, String name, Object value, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureView, trainingDataVersion, name, value);
  }

  public void deleteTag(FeatureView featureView, String name)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureView, name);
  }

  public void deleteTag(FeatureView featureView, String name, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureView, trainingDataVersion, name);
  }

  public Object getTag(FeatureView featureView, String name)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, name);
  }

  public Object getTag(FeatureView featureView, String name, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, trainingDataVersion, name);
  }

  public Map<String, Object> getTags(FeatureView featureView)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView);
  }

  public Map<String, Object> getTags(FeatureView featureView, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, trainingDataVersion);
  }
}
