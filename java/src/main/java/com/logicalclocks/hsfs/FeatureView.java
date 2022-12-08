package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.constructor.Filter;
import com.logicalclocks.hsfs.constructor.FilterLogic;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashSet;
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

  @Getter
  @Setter
  private String type = "featureViewDTO";

  private static FeatureViewEngine featureViewEngine = new FeatureViewEngine();
  private static VectorServer vectorServer = new VectorServer();
  private Integer extraFilterVersion = null;
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

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

    /**
     * Query of a feature view. Note that `as_of` argument in the `Query` will be ignored because feature view does
     * not support time travel query.
     *
     * @param query
     * @return builder
     */
    public FeatureViewBuilder query(Query query) {
      this.query = query;
      if (query.isTimeTravel()) {
        LOGGER.info("`as_of` argument in the `Query` will be ignored because "
            + "feature view does not support time travel query.");
      }
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
    LOGGER.warn("JobWarning: All jobs associated to feature view `" + name + "`, version `"
        + version + "` will be removed.");
    featureViewEngine.delete(this.featureStore, this.name, this.version);
  }

  public static void clean(FeatureStore featureStore, String featureViewName, Integer featureViewVersion)
      throws FeatureStoreException, IOException {
    featureViewEngine.delete(featureStore, featureViewName, featureViewVersion);
  }

  public FeatureView update(FeatureView other) throws FeatureStoreException, IOException {
    return featureViewEngine.update(other);
  }

  public void initServing() throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, false);
  }

  public void initServing(Boolean batch, Boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, batch, external);
  }

  public void initBatchScoring(Integer trainingDatasetVersion) {
    this.extraFilterVersion = trainingDatasetVersion;
  }

  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry);
  }

  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry, external);
  }

  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVectors(this, entry);
  }

  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVectors(this, entry, external);
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
        endTime != null ? FeatureGroupUtils.getDateFromDateString(endTime) : null,
        extraFilterVersion);
  }

  @JsonIgnore
  public Dataset<Row> getBatchData(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    return getBatchData(startTime, endTime, Maps.newHashMap());
  }

  @JsonIgnore
  public Dataset<Row> getBatchData(String startTime, String endTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return featureViewEngine.getBatchData(
        this,
        startTime != null ? FeatureGroupUtils.getDateFromDateString(startTime) : null,
        endTime != null ? FeatureGroupUtils.getDateFromDateString(endTime) : null,
        readOptions,
        extraFilterVersion
    );
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

  public Integer createTrainingData(
      String startTime, String endTime, String description, DataFormat dataFormat
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .dataFormat(dataFormat)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, null).getVersion();
  }

  public Integer createTrainingData(
      String startTime, String endTime, String description, DataFormat dataFormat,
      Boolean coalesce, StorageConnector storageConnector, String location,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> writeOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .dataFormat(dataFormat)
            .coalesce(coalesce)
            .storageConnector(storageConnector)
            .location(location)
            .seed(seed)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, writeOptions).getVersion();
  }

  public Integer createTrainTestSplit(
      Float testSize, String trainStart, String trainEnd, String testStart, String testEnd,
      String description, DataFormat dataFormat
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainTestSplit(testSize, trainEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .dataFormat(dataFormat)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(2)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, null).getVersion();
  }

  public Integer createTrainTestSplit(
      Float testSize, String trainStart, String trainEnd, String testStart, String testEnd,
      String description, DataFormat dataFormat, Boolean coalesce, StorageConnector storageConnector, String location,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> writeOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainTestSplit(testSize, trainEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .dataFormat(dataFormat)
            .coalesce(coalesce)
            .storageConnector(storageConnector)
            .location(location)
            .trainSplit(Split.TRAIN)
            .seed(seed)
            .timeSplitSize(2)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, writeOptions).getVersion();
  }

  public Integer createTrainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description, DataFormat dataFormat
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainValidationTestSplit(validationSize, testSize, trainEnd, validationStart, validationEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .validationSize(validationSize)
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .validationStart(validationStart)
            .validationEnd(validationEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .dataFormat(dataFormat)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(3)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, null).getVersion();
  }

  public Integer createTrainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description, DataFormat dataFormat,
      Boolean coalesce, StorageConnector storageConnector, String location,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> writeOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainValidationTestSplit(validationSize, testSize, trainEnd, validationStart, validationEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .validationSize(validationSize)
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .validationStart(validationStart)
            .validationEnd(validationEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .dataFormat(dataFormat)
            .coalesce(coalesce)
            .storageConnector(storageConnector)
            .location(location)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(3)
            .seed(seed)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return featureViewEngine.createTrainingDataset(this, trainingDataset, writeOptions).getVersion();
  }

  private List<Dataset<Row>> getDataset(TrainingDatasetBundle trainingDatasetBundle, List<String> splits) {
    List<Dataset<Row>> features = Lists.newArrayList();
    List<Dataset<Row>> labels = Lists.newArrayList();
    for (String split: splits) {
      List<Dataset<Row>> featureLabel = trainingDatasetBundle.getDataset(split, true);
      features.add(featureLabel.get(0));
      labels.add(featureLabel.get(1));
    }
    features.addAll(labels);
    return features;
  }

  public void recreateTrainingDataset(Integer version, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureViewEngine.recreateTrainingDataset(this, version, writeOptions);
  }

  public List<Dataset<Row>> getTrainingData(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainingData(version, null);
  }

  public List<Dataset<Row>> getTrainingData(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(), readOptions)
        .getDataset(true);
  }

  public List<Dataset<Row>> getTrainTestSplit(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainTestSplit(version, null);
  }

  public List<Dataset<Row>> getTrainTestSplit(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return getDataset(
        featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(Split.TRAIN, Split.TEST), readOptions),
        Lists.newArrayList(Split.TRAIN, Split.TEST));
  }

  public List<Dataset<Row>> getTrainValidationTestSplit(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainValidationTestSplit(version, null);
  }

  public List<Dataset<Row>> getTrainValidationTestSplit(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return getDataset(
        featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST), readOptions),
        Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST));
  }

  public List<Dataset<Row>> trainingData(
      String startTime, String endTime, String description
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, null).getDataset(true);
  }

  public List<Dataset<Row>> trainingData(
      String startTime, String endTime, String description,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> readOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .eventStartTime(startTime)
            .eventEndTime(endTime)
            .description(description)
            .seed(seed)
            .statisticsConfig(statisticsConfig)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return featureViewEngine.getTrainingDataset(this, trainingDataset, readOptions).getDataset(true);
  }

  public List<Dataset<Row>> trainTestSplit(
      Float testSize, String trainStart, String trainEnd, String testStart, String testEnd,
      String description
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainTestSplit(testSize, trainEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(2)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .build();
    return getDataset(
        featureViewEngine.getTrainingDataset(this, trainingDataset, null),
        Lists.newArrayList(Split.TRAIN, Split.TEST));
  }

  public List<Dataset<Row>> trainTestSplit(
      Float testSize, String trainStart, String trainEnd, String testStart, String testEnd,
      String description, Long seed, StatisticsConfig statisticsConfig, Map<String, String> readOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainTestSplit(testSize, trainEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(2)
            .seed(seed)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return getDataset(
        featureViewEngine.getTrainingDataset(this, trainingDataset, readOptions),
        Lists.newArrayList(Split.TRAIN, Split.TEST));
  }

  public List<Dataset<Row>> trainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainValidationTestSplit(validationSize, testSize, trainEnd, validationStart, validationEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .validationSize(validationSize)
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .validationStart(validationStart)
            .validationEnd(validationEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(3)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .build();
    return getDataset(
        featureViewEngine.getTrainingDataset(this, trainingDataset, null),
        Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST));
  }

  public List<Dataset<Row>> trainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description,
      Long seed, StatisticsConfig statisticsConfig, Map<String, String> readOptions,
      FilterLogic extraFilterLogic, Filter extraFilter
  ) throws IOException, FeatureStoreException, ParseException {
    validateTrainValidationTestSplit(validationSize, testSize, trainEnd, validationStart, validationEnd, testStart);
    TrainingDataset trainingDataset =
        this.featureStore
            .createTrainingDataset()
            .name("") // name is set in the backend
            .validationSize(validationSize)
            .testSize(testSize)
            .trainStart(trainStart)
            .trainEnd(trainEnd)
            .validationStart(validationStart)
            .validationEnd(validationEnd)
            .testStart(testStart)
            .testEnd(testEnd)
            .description(description)
            .trainSplit(Split.TRAIN)
            .timeSplitSize(3)
            .seed(seed)
            .trainingDatasetType(TrainingDatasetType.IN_MEMORY_TRAINING_DATASET)
            .statisticsConfig(statisticsConfig)
            .extraFilterLogic(extraFilterLogic)
            .extraFilter(extraFilter)
            .build();
    return getDataset(
        featureViewEngine.getTrainingDataset(this, trainingDataset, readOptions),
        Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST));
  }

  private void validateTrainTestSplit(Float testSize, String trainEnd, String testStart) throws FeatureStoreException {
    if (!((testSize != null && testSize > 0 && testSize < 1)
        || (!Strings.isNullOrEmpty(trainEnd) || !Strings.isNullOrEmpty(testStart)))) {
      throw new FeatureStoreException(
          "Invalid split input."
              + "You should specify either `testSize` or (`trainEnd` or `testStart`)."
              + " `testSize` should be between 0 and 1 if specified."
      );
    }
  }

  private void validateTrainValidationTestSplit(
      Float validationSize, Float testSize, String trainEnd, String validationStart, String validationEnd,
      String testStart)
      throws FeatureStoreException {
    if (!((validationSize != null && validationSize > 0 && validationSize < 1
        && testSize != null && testSize > 0 && testSize < 1
        && validationSize + testSize < 1)
        || ((!Strings.isNullOrEmpty(trainEnd) || !Strings.isNullOrEmpty(validationStart))
        && (!Strings.isNullOrEmpty(validationEnd) || !Strings.isNullOrEmpty(testStart))))) {
      throw new FeatureStoreException(
          "Invalid split input."
              + " You should specify either (`validationSize` and `testSize`) or "
              + "((`trainEnd` or `validationStart`) and (`validationEnd` "
              + "or `testStart`))."
              + "`validationSize`, `testSize` and sum of `validationSize` and `testSize` should be between 0 and 1 "
              + "if specified."
      );
    }
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

  /**
   * Set of primary key names that is used as keys in input dict object for `get_serving_vector` method.
   *
   * @return Set of serving keys
   * @throws SQLException
   * @throws IOException
   * @throws FeatureStoreException
   * @throws ClassNotFoundException
   */
  @JsonIgnore
  public HashSet<String> getPrimaryKeys()
      throws SQLException, IOException, FeatureStoreException, ClassNotFoundException {
    if (vectorServer.getServingKeys().isEmpty()) {
      initServing();
    }
    return vectorServer.getServingKeys();
  }
}
