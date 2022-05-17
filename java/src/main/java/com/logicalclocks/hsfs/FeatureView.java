package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.hsfs.constructor.Query;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class FeatureView {

  @Getter
  @Setter
  @JsonIgnore
  private Integer id;

  @Getter
  @Setter
  private String name;

  @Getter
  @Setter
  private Integer version;

  @Getter
  @Setter
  private String description;

  @Getter
  @Setter
  private List<TrainingDatasetFeature> features;

  @Getter
  @Setter
  @JsonIgnore
  private FeatureStore featureStore;

  @Getter
  @Setter
  @JsonProperty("queryDTO")
  private Query query;

  @Getter
  @Setter
  @JsonIgnore
  private List<String> label;

  @Builder
  public FeatureView(@NonNull String name, Integer version, @NonNull Query query, String description,
      @NonNull FeatureStore featureStore, List<String> label) {
    // TODO: add transformation function
    this.name = name;
    this.version = version;
    this.query = query;
    this.description = description;
    this.featureStore = featureStore;
    this.label = label != null ? label.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
  }
  
  public void delete() {
    
  }
  
  public FeatureView update(FeatureView other) {
    return this;
  }

  public void initServing() {

  }

  public void initServing(Integer trainingDatasetVersion) {

  }

  public void initServing(Boolean batch, Boolean external) {

  }
  
  public void initServing(Integer trainingDatasetVersion, Boolean batch, Boolean external) {
    
  }

  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry) {
    return null;
  }
  
  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry, boolean external) {
    return null;
  }

  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry) {
    return null;
  }

  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry, boolean external) {
    return null;
  }

  public List<Object> previewFeatureVector() {
    return null;
  }

  public List<Object> previewFeatureVector(boolean external) {
    return null;
  }

  public List<List<Object>> previewFeatureVectors(Integer n) {
    return null;
  }

  public List<List<Object>> previewFeatureVectors(Integer n, boolean external) {
    return null;
  }

  @JsonIgnore
  public String getBatchQuery(String startTime, String endTime) {
    return "";
  }

  public Dataset<Row> getBatchData(String startTime, String endTime) {
    return null;
  }

  public Dataset<Row> getBatchData(String startTime, String endTime, Map<String, String> readOptions) {
    return null;
  }

  /**
   * Add name/value tag to the feature view.
   *
   * @param name  name of the tag
   * @param value value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
  }

  /**
   * Get all tags of the feature view.
   *
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, Object> getTags() throws FeatureStoreException, IOException {
    return null;
  }

  /**
   * Get a single tag value of the feature view.
   *
   * @param name name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Object getTag(String name) throws FeatureStoreException, IOException {
    return null;
  }

  /**
   * Delete a tag of the feature view.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
  }

  public TrainingDatasetRepository getTrainingDataset(
      Integer version, String description, Map<String, Float> splits,
      Long seed, StatisticsConfig statisticsConfig
  ) {
    return null;
  }

  public TrainingDatasetRepository createTrainingDataset(
      Integer version, String description, DataFormat dataFormat,
      Boolean coalesce, StorageConnector storageConnector, String location, Map<String, Float> splits,
      Long seed, StatisticsConfig statisticsConfig
  ) {
    return null;
  }

  public void recreateTrainingDataset(Integer version) {

  }

  public void purgeTrainingData(Integer version) {

  }

  public void purgeAllTrainingData() {

  }

  public void deleteTrainingDataset(Integer version) {

  }

  public void deleteAllTrainingDatasets() {

  }

  /**
   * Add name/value tag to the training dataset.
   *
   * @param name  name of the tag
   * @param value value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTrainingDatasetTag(String name, Object value) throws FeatureStoreException, IOException {
  }

  /**
   * Get all tags of the training dataset.
   *
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, Object> getTrainingDatasetTags() throws FeatureStoreException, IOException {
    return null;
  }

  /**
   * Get a single tag value of the training dataset.
   *
   * @param name name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Object getTrainingDatasetTag(String name) throws FeatureStoreException, IOException {
    return null;
  }

  /**
   * Delete a tag of the training dataset.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTrainingDatasetTag(String name) throws FeatureStoreException, IOException {
  }

}
