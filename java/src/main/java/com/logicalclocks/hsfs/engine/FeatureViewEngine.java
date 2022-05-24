package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.TrainingDatasetBundle;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.metadata.FeatureViewApi;
import com.logicalclocks.hsfs.metadata.TagsApi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureViewEngine {

  private FeatureViewApi featureViewApi = new FeatureViewApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_VIEW);

  public FeatureView save(FeatureView featureView) throws FeatureStoreException, IOException {
    featureView.setFeatures(makeLabelFeatures(featureView.getLabels()));
    FeatureView updatedFeatureView = featureViewApi.save(featureView);
    featureView.setVersion(updatedFeatureView.getVersion());
    featureView.setFeatures(updatedFeatureView.getFeatures());
    return featureView;
  }

  private List<TrainingDatasetFeature> makeLabelFeatures(List<String> labels) {
    return labels.stream().map(label -> new TrainingDatasetFeature(label.toLowerCase(), true))
        .collect(Collectors.toList());
  }

  public FeatureView update(FeatureView featureView) throws FeatureStoreException,
      IOException {
    FeatureView featureViewUpdated = featureViewApi.update(featureView);
    featureView.setDescription(featureViewUpdated.getDescription());
    return featureView;
  }

  public FeatureView get(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    return featureViewApi.get(featureStore, name, version);
  }

  public List<FeatureView> get(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    return featureViewApi.get(featureStore, name);
  }

  public void delete(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStore, name);
  }

  public void delete(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStore, name, version);
  }

  public void createTrainingDataset(
      FeatureView featureView, TrainingDataset trainingDataset, Map<String, String> userWriteOptions
  ) {
  }

  public TrainingDatasetBundle getTrainingDataset(
      FeatureView featureView, TrainingDataset trainingDataset, Map<String, String> userWriteOptions
  ) {
    return null;
  }

  public void recreateTrainingDataset(Integer version) {

  }

  public void deleteTrainingData(FeatureView featureView, Integer trainingDataVersion) {

  }

  public void deleteTrainingData(FeatureView featureView) {

  }

  public void deleteTrainingDatasetOnly(FeatureView featureView, Integer trainingDataVersion) {

  }

  public void deleteTrainingDatasetOnly(FeatureView featureView) {

  }

  public String getBatchQueryString(FeatureView featureView) {
    return null;
  }

  public String getBatchQueryString(
      FeatureView featureView, String startTime, String endTime
  ) {
    return null;
  }

  public Dataset<Row> getBatchData(
      FeatureView featureView, String startTime, String endTime, Map<String, String> readOptions
  ) {
    return null;
  }

  public void addTag(FeatureView featureView, String name, Object value) {

  }

  public void addTag(FeatureView featureView, String name, Object value, Integer trainingDataVersion) {

  }

  public void deleteTag(FeatureView featureView, String name) {

  }

  public void deleteTag(FeatureView featureView, String name, Integer trainingDataVersion) {

  }

  public Object getTag(FeatureView featureView, String name) {
    return null;
  }

  public Object getTag(FeatureView featureView, String name, Integer trainingDataVersion) {
    return null;
  }

  public Map<String, Object> getTags(FeatureView featureView) {
    return null;
  }

  public Map<String, Object> getTags(FeatureView featureView, Integer trainingDataVersion) {
    return null;
  }
}
