package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
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
import java.text.ParseException;
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

  public void initServing(Boolean batch, Boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, batch, external);
  }

  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry, false);
  }

  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry, external);
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
    return previewFeatureVectors(1, false).get(0);
  }

  public List<Object> previewFeatureVector(boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return previewFeatureVectors(1, external).get(0);
  }

  public List<List<Object>> previewFeatureVectors(Integer n)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.previewFeatureVectors(this, false, n);

  }

  public List<List<Object>> previewFeatureVectors(Integer n, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.previewFeatureVectors(this, external, n);

  }

  @JsonIgnore
  public String getBatchQuery() throws FeatureStoreException, IOException, ParseException {
    return getBatchQuery(null, null);
  }

  @JsonIgnore
  public String getBatchQuery(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    return featureViewEngine.getBatchQueryString(
        this,
        startTime != null ? FeatureGroupUtils.getDateFromDateString(startTime) : null,
        endTime != null ? FeatureGroupUtils.getDateFromDateString(endTime) : null);
  }

  @JsonIgnore
  public Dataset<Row> getBatchData(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    return getBatchData(startTime, endTime, Maps.newHashMap());
  }

  public Dataset<Row> getBatchData(String startTime, String endTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return featureViewEngine.getBatchData(
        this,
        startTime != null ? FeatureGroupUtils.getDateFromDateString(startTime) : null,
        endTime != null ? FeatureGroupUtils.getDateFromDateString(endTime) : null,
        readOptions);

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

  public TrainingDatasetBundle getTrainingDataset(Integer version)
      throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .version(version)
            .build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, Maps.newHashMap());
  }

  public TrainingDatasetBundle getTrainingDataset(String startTime, String endTime)
      throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, Maps.newHashMap());
  }

  public TrainingDatasetBundle getTrainingDataset(
      String startTime, String endTime, Map<String, Float> splits, String trainSplit
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .splits(splits != null
                ? splits.entrySet().stream().map(entry -> new Split(entry.getKey(), entry.getValue())).collect(
                Collectors.toList()) : null)
            .trainSplit(trainSplit)
            .build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, Maps.newHashMap());
  }

  public TrainingDatasetBundle getTrainingDataset(
      Integer version, String startTime, String endTime, String description, Map<String, Float> splits,
      String trainSplit, StatisticsConfig statisticsConfig, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .version(version)
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .splits(splits != null
                ? splits.entrySet().stream().map(entry -> new Split(entry.getKey(), entry.getValue())).collect(
                Collectors.toList()) : null)
            .trainSplit(trainSplit)
            .statisticsConfig(statisticsConfig)
            .build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, readOptions);
  }

  public TrainingDatasetBundle createTrainingDataset(
      String startTime, String endTime, DataFormat dataFormat, StorageConnector storageConnector
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .dataFormat(dataFormat)
            .storageConnector(storageConnector)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, Maps.newHashMap());
  }

  public TrainingDatasetBundle createTrainingDataset(
      String startTime, String endTime, DataFormat dataFormat, StorageConnector storageConnector,
      Map<String, Float> splits, String trainSplit
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .dataFormat(dataFormat)
            .storageConnector(storageConnector)
            .splits(splits.entrySet().stream().map(entry -> new Split(entry.getKey(), entry.getValue())).collect(
                Collectors.toList()))
            .trainSplit(trainSplit)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, Maps.newHashMap());
  }

  public TrainingDatasetBundle createTrainingDataset(
      Integer version, String startTime, String endTime, String description, DataFormat dataFormat,
      Boolean coalesce, StorageConnector storageConnector, String location, Map<String, Float> splits,
      String trainSplit, Long seed, StatisticsConfig statisticsConfig, Map<String, String> writeOptions
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .version(version)
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .dataFormat(dataFormat)
            .coalesce(coalesce)
            .storageConnector(storageConnector)
            .location(location)
            .splits(splits != null
                ? splits.entrySet().stream().map(entry -> new Split(entry.getKey(), entry.getValue())).collect(
                Collectors.toList()) : null)
            .trainSplit(trainSplit)
            .seed(seed)
            .statisticsConfig(statisticsConfig)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, writeOptions);
  }

  public void recreateTrainingDataset(Integer version, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureViewEngine.recreateTrainingDataset(this, version, writeOptions);
  }

  public void purgeTrainingData(Integer version) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingDatasetOnly(this, version);
  }

  public void purgeAllTrainingData() throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingDatasetOnly(this);
  }

  public void deleteTrainingDataset(Integer version) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingData(this, version);
  }

  public void deleteAllTrainingDatasets() throws FeatureStoreException, IOException {
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
  public void addTrainingDatasetTag(Integer version, String name, Object value) throws FeatureStoreException,
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
  public Object getTrainingDatasetTag(Integer version, String name) throws FeatureStoreException, IOException {
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
  public void deleteTrainingDatasetTag(Integer version, String name) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTag(this, name, version);
  }

}
