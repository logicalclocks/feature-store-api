package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.engine.VectorServer;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.sql.SQLException;
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
  @JsonProperty("query")
  private Query query;

  @Getter
  @Setter
  @JsonIgnore
  private List<String> labels;

  private static FeatureViewEngine featureViewEngine = new FeatureViewEngine();
  private static VectorServer vectorServer = new VectorServer();

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

    public FeatureViewBuilder name(String name) {
      this.name = name;
      return this;
    }

    public FeatureViewBuilder version(Integer version) {
      this.version = version;
      return this;
    }

    public FeatureViewBuilder description(String description) {
      this.description = description;
      return this;
    }

    public FeatureViewBuilder query(Query query) {
      this.query = query;
      return this;
    }

    public FeatureViewBuilder labels(List<String> labels) {
      this.labels = labels;
      return this;
    }

    public FeatureView build() throws FeatureStoreException, IOException {
      FeatureView featureView = new FeatureView(name, version, query, description, featureStore, labels);
      featureViewEngine.save(featureView);
      return featureView;
    }
  }

  public FeatureView(@NonNull String name, Integer version, @NonNull Query query, String description,
      @NonNull FeatureStore featureStore, List<String> labels) {
    // TODO: add transformation function
    this.name = name;
    this.version = version;
    this.query = query;
    this.description = description;
    this.featureStore = featureStore;
    this.labels = labels != null ? labels.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
  }

  public void delete() throws FeatureStoreException, IOException {
    featureViewEngine.delete(this.featureStore, this.name, this.version);
  }

  public FeatureView update(FeatureView other) throws FeatureStoreException, IOException {
    return featureViewEngine.update(other);
  }

  public void initServing() throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, false, false);
  }

  public void initServing(Integer trainingDatasetVersion)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, trainingDatasetVersion, false, false);
  }

  public void initServing(Boolean batch, Boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, batch, external);
  }

  public void initServing(Integer trainingDatasetVersion, Boolean batch, Boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, trainingDatasetVersion, batch, external);
  }

  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry, false);
  }

  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry, false);
  }

  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVectors(this, entry, false);
  }

  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVectors(this, entry, external);
  }

  public List<Object> previewFeatureVector()
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.previewFeatureVector(this, false);
  }

  public List<Object> previewFeatureVector(boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.previewFeatureVector(this, external);
  }

  public List<List<Object>> previewFeatureVectors(Integer n)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.previewFeatureVectors(this, false);

  }

  public List<List<Object>> previewFeatureVectors(Integer n, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.previewFeatureVectors(this, external);

  }

  @JsonIgnore
  public String getBatchQuery(String startTime, String endTime) {
    return featureViewEngine.getBatchQueryString(this, startTime, endTime);
  }

  @JsonIgnore
  public Dataset<Row> getBatchData(String startTime, String endTime) {
    return featureViewEngine.getBatchData(this, startTime, endTime, Maps.newHashMap());
  }

  public Dataset<Row> getBatchData(String startTime, String endTime, Map<String, String> readOptions) {
    return featureViewEngine.getBatchData(this, startTime, endTime, readOptions);

  }

  /**
   * Add name/value tag to the feature view.
   *
   * @param name
   *     name of the tag
   * @param value
   *     value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
    featureViewEngine.addTag(this, name, value);
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
    return featureViewEngine.getTags(this);
  }

  /**
   * Get a single tag value of the feature view.
   *
   * @param name
   *     name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Object getTag(String name) throws FeatureStoreException, IOException {
    return featureViewEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the feature view.
   *
   * @param name
   *     name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTag(this, name);
  }

  public TrainingDatasetRepository getTrainingDataset(
      Integer version, String description, Map<String, Float> splits,
      StatisticsConfig statisticsConfig, Map<String, String> writeOptions
  ) {
    TrainingDataset trainingDataset = new FeatureStore().createTrainingDataset().build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, writeOptions);
  }

  public TrainingDatasetRepository createTrainingDataset(
      Integer version, String description, DataFormat dataFormat,
      Boolean coalesce, StorageConnector storageConnector, String location, Map<String, Float> splits,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> writeOptions
  ) {
    TrainingDataset trainingDataset = new FeatureStore().createTrainingDataset().build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, writeOptions);
  }

  public void recreateTrainingDataset(Integer version) {
    // TODO:
  }

  public void purgeTrainingData(Integer version) {
    featureViewEngine.deleteTrainingDatasetOnly(this, version);
  }

  public void purgeAllTrainingData() {
    featureViewEngine.deleteTrainingDatasetOnly(this);
  }

  public void deleteTrainingDataset(Integer version) {
    featureViewEngine.deleteTrainingData(this, version);
  }

  public void deleteAllTrainingDatasets() {
    featureViewEngine.deleteTrainingData(this);
  }

  /**
   * Add name/value tag to the training dataset.
   *
   * @param name
   *     name of the tag
   * @param value
   *     value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTrainingDatasetTag(String name, Object value, Integer version) throws FeatureStoreException,
      IOException {
    featureViewEngine.addTag(this, name, value, version);
  }

  /**
   * Get all tags of the training dataset.
   *
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, Object> getTrainingDatasetTags(Integer version) throws FeatureStoreException, IOException {
    return featureViewEngine.getTags(this, version);
  }

  /**
   * Get a single tag value of the training dataset.
   *
   * @param name
   *     name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Object getTrainingDatasetTag(String name, Integer version) throws FeatureStoreException, IOException {
    return featureViewEngine.getTag(this, name, version);
  }

  /**
   * Delete a tag of the training dataset.
   *
   * @param name
   *     name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTrainingDatasetTag(String name, Integer version) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTag(this, name, version);
  }

}
