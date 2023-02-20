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
     * @param query Query object
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

  /**
   * Delete current feature view, all associated metadata and training data.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature view handle
   *        FeatureView fv = ...;
   *        // delete feature view
   *        fv.delete();
   * }
   * </pre>
   *
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void delete() throws FeatureStoreException, IOException {
    LOGGER.warn("JobWarning: All jobs associated to feature view `" + name + "`, version `"
        + version + "` will be removed.");
    featureViewEngine.delete(this.featureStore, this.name, this.version);
  }

  /**
   * Delete the feature view and all associated metadata and training data. This can delete corrupted feature view
   * which cannot be retrieved due to a corrupted query for example.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature view handle
   *        FeatureView fv = ...;
   *        // delete feature view
   *        fv.clean();
   * }
   * </pre>
   *
   * @param featureStore Feature store metadata object.
   * @param featureViewName Name of feature view.
   * @param featureViewVersion Version of feature view.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public static void clean(FeatureStore featureStore, String featureViewName, Integer featureViewVersion)
      throws FeatureStoreException, IOException {
    featureViewEngine.delete(featureStore, featureViewName, featureViewVersion);
  }

  /**
   * Update the description of the feature view.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature view handle
   *        FeatureView fv = ...;
   *        // update with new description
   *        fv.setDescription("this is new description");
   *        // delete feature view
   *        fv.update(fv);
   * }
   * </pre>
   *
   * @param other Updated FeatureView metadata Object.
   * @return FeatureView Metadata Object.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public FeatureView update(FeatureView other) throws FeatureStoreException, IOException {
    return featureViewEngine.update(other);
  }

  /**
   * Initialise feature view to retrieve feature vector from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature view handle
   *        FeatureView fv = ...;
   *        // Initialise feature view serving
   *        fv.initServing();
   * }
   * </pre>
   *
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  public void initServing() throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, false);
  }

  /**
   * Initialise feature view to retrieve feature vector from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature view handle
   *        FeatureView fv = ...;
   *        // Initialise feature view batch serving
   *        fv.initServing(true, false);
   * }
   * </pre>
   *
   * @param batch Whether to initialise feature view to retrieve feature vector from offline feature store.
   * @param external If set to `true`, the connection to the online feature store is established using the same host as
   *                 for the `host` parameter in the connection object.
   *                 If set to False, the online feature store storage connector is used which relies on the private IP.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @Deprecated
  public void initServing(Boolean batch, Boolean external)
      throws FeatureStoreException, IOException, SQLException, ClassNotFoundException {
    vectorServer.initServing(this, batch, external);
  }

  /**
   * Initialise feature view to retrieve feature vector from offline feature store.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Initialise feature view batch scoring
   *        fv.initBatchScoring(1);
   * }
   * </pre>
   *
   * @param trainingDatasetVersion Version of training dataset to identify additional filters attached to the training
   *                               dataset.
   */
  public void initBatchScoring(Integer trainingDatasetVersion) {
    this.extraFilterVersion = trainingDatasetVersion;
  }

  /**
   * Returns assembled feature vector from online feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature view handle
   *        FeatureView fv = ...;
   *        // define primary key values to fetch data from online feature store
   *        Map<String, Int> entry = ...;
   *        // get feature vector
   *        fv.getFeatureVector(entry);
   * }
   * </pre>
   *
   * @param entry Fictionary of feature group primary key and values provided by serving application.
   * @return List of feature values related to provided primary keys, ordered according to positions of the features
   *         in the feature view query.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry);
  }

  /**
   * Returns assembled feature vector from online feature store.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // define primary key values to fetch data from online feature store
   *        Map<String, Long> entry = ...;
   *        //Get feature vector
   *        fv.getFeatureVector(entry, false);
   * }
   * </pre>
   *
   * @param entry dictionary of feature group primary key and values provided by serving application.
   * @param external If set to true, the connection to the online feature store is established using the same host as
   *                 for the `host` parameter in the [`hsfs.connection()`](connection_api.md#connection) method.
   *                 If set to false, the online feature store storage connector is used which relies on the private IP.
   *                 Defaults to True if connection to Hopsworks is established from external environment
   * @return List of feature values related to provided primary keys, ordered according to positions of this features
   *         in the feature view query.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @JsonIgnore
  public List<Object> getFeatureVector(Map<String, Object> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVector(this, entry, external);
  }


  /**
   * Returns assembled feature vectors in batches from online feature store.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // define primary key values to fetch data from online feature store
   *        Map<String, List<Long>> entry = ...;
   *        //Get feature vector
   *        fv.getFeatureVector(entry);
   * }
   * </pre>
   *
   * @param entry a list of dictionary of feature group primary key and values provided by serving application.
   * @return List of lists of feature values related to provided primary keys, ordered according to
   *         positions of this features in the feature view query.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVectors(this, entry);
  }

  /**
   *Returns assembled feature vectors in batches from online feature store.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // define primary key values to fetch data from online feature store
   *        Map<String, List<Long>> entry = ...;
   *        //Get feature vector
   *        fv.getFeatureVectors(entry, false);
   * }
   * </pre>
   *
   * @param entry a list of dictionary of feature group primary key and values provided by serving application.
   * @param external If set to `true`, the connection to the  online feature store is established using the same host as
   *                 for the `host` parameter in the [`hsfs.connection()`](connection_api.md#connection) method.
   *                 If set to False, the online feature store storage connector is used which relies on the private IP.
   * @return List of lists of feature values related to provided primary keys, ordered according to
   *         positions of this features in the feature view query.
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
   */
  @JsonIgnore
  public List<List<Object>> getFeatureVectors(Map<String, List<Object>> entry, boolean external)
      throws SQLException, FeatureStoreException, IOException, ClassNotFoundException {
    return vectorServer.getFeatureVectors(this, entry, external);
  }

  /**
   * Get a query string of the batch query.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Get batch query
   *        fv.getBatchQuery();
   * }
   * </pre>
   *
   * @return batch query
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  @JsonIgnore
  public String getBatchQuery() throws FeatureStoreException, IOException, ParseException {
    return getBatchQuery(null, null);
  }

  /**
   * Get a query string of the batch query.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Get batch query that will fetch data from jan 1, 2023 to Jan 31, 2023
   *        fv.getBatchQuery("20230101", "20130131");
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return batch query
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
  @JsonIgnore
  public String getBatchQuery(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    return featureViewEngine.getBatchQueryString(
        this,
        startTime != null ? FeatureGroupUtils.getDateFromDateString(startTime) : null,
        endTime != null ? FeatureGroupUtils.getDateFromDateString(endTime) : null,
        extraFilterVersion);
  }

  /**
   * Get a batch of data from an event time interval from the offline feature store.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Get batch query that will fetch data from jan 1, 2023 to Jan 31, 2023
   *        fv.getBatchQuery("20230101", "20130131");
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @return Spark dataframe of batch data.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
  @JsonIgnore
  public Dataset<Row> getBatchData(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    return getBatchData(startTime, endTime, Maps.newHashMap());
  }

  /**
   * Get a batch of data from an event time interval from the offline feature store.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Get batch query that will fetch data from jan 1, 2023 to Jan 31, 2023
   *        fv.getBatchQuery("20230101", "20130131");
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param readOptions Additional read options as key/value pairs.
   * @return Spark dataframe of batch data
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
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
   * A tag consists of a name and value pair. Tag names are unique identifiers across the whole cluster. The value of a
   * tag can be any valid json - primitives, arrays or json objects.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //attach a tag to a feature view
   *        JSONObject value = ...;
   *        fv.addTag("tag_schema", value);
   * }
   * </pre>
   *
   * @param name
   *     name of the tag
   * @param value
   *     value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks;
   * @throws IOException Generic IO exception.
   */
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
    featureViewEngine.addTag(this, name, value);
  }

  /**
   * Get all tags of the feature view.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //get tags
   *        fv.getTags();
   * }
   * </pre>
   *
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks;
   * @throws IOException Generic IO exception.
   */
  @JsonIgnore
  public Map<String, Object> getTags() throws FeatureStoreException, IOException {
    return featureViewEngine.getTags(this);
  }

  /**
   * Get a single tag value of the feature view.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //get tag
   *        fv.getTag("tag_name");
   * }
   * </pre>
   *
   * @param name
   *     name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks;
   * @throws IOException Generic IO exception.
   */
  @JsonIgnore
  public Object getTag(String name) throws FeatureStoreException, IOException {
    return featureViewEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the feature view.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //delete tag
   *        fv.deleteTag("tag_name");
   * }
   * </pre>
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException If Client is not connected to Hopsworks;
   * @throws IOException Generic IO exception.
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTag(this, name);
  }

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data can be retrieved by calling `feature_view.getTrainingData()`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset
   *        String startTime = "20220101000000";
   *        String endTime = "20220606235959";
   *        String description = "demo training dataset":
   *        fv.createTrainingData(startTime, endTime, description, DataFormat.CSV);
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param dataFormat  The data format used to save the training dataset.
   * @return Training dataset version
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data can be retrieved by calling `feature_view.getTrainingData()`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset
   *        String startTime = "20220101000000";
   *        String endTime = "20220606235959";
   *        String description = "demo training dataset":
   *        String location = ...;
   *        StatisticsConfig statisticsConfig = ...;
   *        fv.createTrainingData(startTime, endTime, description, DataFormat.CSV, true, location, statisticsConfig);
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param dataFormat  The data format used to save the training dataset.
   * @param coalesce If true the training dataset data will be coalesced into a single partition before writing.
   *                 The resulting training dataset will be a single file per split.
   * @param storageConnector Storage connector defining the sink location for the  training dataset. If  `null` is
   *                         provided  and materializes training dataset on HopsFS.
   * @param location Path to complement the sink storage connector with, e.g if the storage connector points to an
   *                 S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training
   *                 dataset.  If empty string is provided `""`, saving the training dataset at the root defined by the
   *                 storage connector.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param writeOptions Additional write options as key-value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return Training dataset version.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided `startTime`/`endTime` date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided `startTime`/`endTime` strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data is split into train and test set at random or according to time ranges. The training data can be retrieved by
   * calling `feature_view.getTrainTestSplit` method.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String testStart = "20220701000000";
   *        String testEnd = "20220830235959";
   *        String description = "demo training dataset":
   *        fv.createTrainTestSplit(null, trainStart, trainEnd, testStart, testEnd, description, DataFormat.CSV);
   *
   *        //or based on random split
   *        fv.createTrainTestSplit(30, null, null, null, null, description, DataFormat.CSV);
   * }
   * </pre>
   *
   * @param testSize size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param dataFormat  The data format used to save the training dataset.
   * @return Training dataset version
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data is split into train and test set at random or according to time ranges. The training data can be retrieved by
   * calling `feature_view.getTrainTestSplit` method.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String testStart = "20220701000000";
   *        String testEnd = "20220830235959";
   *        String description = "demo training dataset":
   *        fv.createTrainTestSplit(null, trainStart, trainEnd, testStart, testEnd, description, DataFormat.CSV);
   *
   *        //or based on random split
   *        fv.createTrainTestSplit(30, null, null, null, null, description, DataFormat.CSV);
   * }
   * </pre>
   *
   * @param testSize size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param dataFormat  The data format used to save the training dataset.
   * @param coalesce If true the training dataset data will be coalesced into a single partition before writing.
   *                 The resulting training dataset will be a single file per split.
   * @param storageConnector Storage connector defining the sink location for the  training dataset. If  `null` is
   *                         provided  and materializes training dataset on HopsFS.
   * @param location Path to complement the sink storage connector with, e.g if the storage connector points to an
   *                 S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training
   *                 dataset.  If empty string is provided `""`, saving the training dataset at the root defined by the
   *                 storage connector.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param writeOptions Additional write options as key-value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return Training dataset version
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data is split into train, validation, and test set at random or according to time range. The training data can be
   * retrieved by calling `feature_view.getTrainValidationTestSplit`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String validationStart = "20220701000000";
   *        String validationEnd = "20220830235959";
   *        String testStart = "20220901000000";
   *        String testEnd = "20220931235959";
   *        String description = "demo training dataset":
   *        fv.createTrainTestSplit(null, null, trainStart, trainEnd, validationStart, validationEnd, testStart,
   *        testEnd, description, DataFormat.CSV);
   *
   *        //or based on random split
   *        fv.createTrainTestSplit(20, 10, null, null, null, null, null, null, description, DataFormat.CSV);
   * }
   * </pre>
   *
   * @param validationSize size of validation set.
   * @param testSize size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                        `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                      `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param dataFormat  The data format used to save the training dataset.
   * @return Training dataset version
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and save the corresponding training data into `location`. The training
   * data is split into train, validation, and test set at random or according to time range. The training data can be
   * retrieved by calling `feature_view.getTrainValidationTestSplit`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String validationStart = "20220701000000";
   *        String validationEnd = "20220830235959";
   *        String testStart = "20220901000000";
   *        String testEnd = "20220931235959";
   *        String description = "demo training dataset":
   *        StorageConnector storageConnector = ...;
   *        String location = ..;
   *        Long seed = ...;
   *        Boolean coalesce = true;
   *        StatisticsConfig statisticsConfig = ...;
   *        Map<String, String> writeOptions = ...;
   *        FilterLogic extraFilterLogic = ...,
   *        Filter extraFilter = ...;
   *        fv.createTrainTestSplit(null, null, trainStart, trainEnd, validationStart, validationEnd, testStart,
   *        testEnd,  description, DataFormat.CSV, coalesce, storageConnector, location, seed, statisticsConfig,
   *        writeOptions, extraFilterLogic, extraFilter);
   *
   *        //or based on random split
   *        fv.createTrainTestSplit(20, 10, null, null, null, null, null, null, description, DataFormat.CSV, coalesce,
   *        storageConnector, location, seed, statisticsConfig, writeOptions, extraFilterLogic, extraFilter);
   * }
   * </pre>
   *
   * @param validationSize size of validation set.
   * @param testSize size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                        `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                      `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param dataFormat  The data format used to save the training dataset.
   * @param coalesce If true the training dataset data will be coalesced into a single partition before writing.
   *                 The resulting training dataset will be a single file per split.
   * @param storageConnector Storage connector defining the sink location for the  training dataset. If  `null` is
   *                         provided  and materializes training dataset on HopsFS.
   * @param location Path to complement the sink storage connector with, e.g if the storage connector points to an
   *                 S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training
   *                 dataset.  If empty string is provided `""`, saving the training dataset at the root defined by the
   *                 storage connector.
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param writeOptions Additional write options as key-value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return Training dataset version.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Recreate a training dataset.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //define write options
   *        Map<String, String> writeOptions = ...;
   *        //recreate training data
   *        fv.recreateTrainingDataset(1, writeOptions);
   * }
   * </pre>
   *
   * @param version training dataset version.
   * @param writeOptions Additional read options as key-value pairs.
   * @throws FeatureStoreException If Client is not connected to Hopsworks,
   * @throws IOException Generic IO exception.
   */
  public void recreateTrainingDataset(Integer version, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureViewEngine.recreateTrainingDataset(this, version, writeOptions);
  }

  /**
   * Get training data created by `featureView.createTrainingData` or `featureView.trainingData`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //get training data
   *        fv.getTrainingData(1);
   * }
   * </pre>
   *
   * @param version training dataset version.
   * @return List of dataframe of features and labels
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainingData(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainingData(version, null);
  }

  /**
   * Get training data created by `featureView.createTrainingData` or `featureView.trainingData`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //define write options
   *        Map<String, String> writeOptions = ...;
   *        //get training data
   *        fv.getTrainingData(1, writeOptions);
   * }
   * </pre>
   *
   * @param version training dataset version.
   * @param readOptions Additional read options as key/value pairs.
   * @return List of dataframe of features and labels
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainingData(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(), readOptions)
        .getDataset(true);
  }

  /**
   * Get training data created by `featureView.createTrainTestSplit`  or `featureView.trainTestSplit`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //get train test split dataframe of features and labels
   *        fv.getTrainTestSplit(1);
   * }
   * </pre>
   *
   * @param version training dataset version.
   * @return List of dataframe of features and labels
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainTestSplit(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainTestSplit(version, null);
  }

  /**
   * Get training data created by `featureView.createTrainTestSplit`  or `featureView.trainTestSplit`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // define additional readOptions
   *        Map<String, String> readOptions = ...;
   *        //get train test split dataframe of features and labels
   *        fv.getTrainTestSplit(1, readOptions);
   * }
   * </pre>
   *
   * @param version training dataset version.
   * @param readOptions Additional read options as key/value pairs.
   * @return List of dataframe of features and labels
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainTestSplit(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return getDataset(
        featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(Split.TRAIN, Split.TEST), readOptions),
        Lists.newArrayList(Split.TRAIN, Split.TEST));
  }

  /**
   * Get training data created by `featureView.createTrainValidationTestSplit` or featureView.trainValidationTestSplit`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //get train, validation, test split dataframe of features and labels
   *        fv.getTrainValidationTestSplit(1);
   * }
   * </pre>
   *
   * @param version training dataset version
   * @return List of dataframe of features and labels
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainValidationTestSplit(
      Integer version
  ) throws IOException, FeatureStoreException, ParseException {
    return getTrainValidationTestSplit(version, null);
  }

  /**
   * Get training data created by `featureView.createTrainValidationTestSplit` or featureView.trainValidationTestSplit`.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // define additional readOptions
   *        Map<String, String> readOptions = ...;
   *        //get train, validation, test split dataframe of features and labels
   *        fv.getTrainValidationTestSplit(1, readOptions);
   * }
   * </pre>
   *
   * @param version training dataset version
   * @param readOptions Additional read options as key/value pairs.
   * @return List of dataframe of features and labels
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify
   *                               date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse strings dates to date types.
   */
  public List<Dataset<Row>> getTrainValidationTestSplit(
      Integer version, Map<String, String> readOptions
  ) throws IOException, FeatureStoreException, ParseException {
    return getDataset(
        featureViewEngine.getTrainingDataset(
            this, version, Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST), readOptions),
        Lists.newArrayList(Split.TRAIN, Split.VALIDATION, Split.TEST));
  }

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data can be
   * recreated by calling `featureView.getTrainingData` with the metadata created.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String startTime = "20220101000000";
   *        String endTime = "20220630235959";
   *        String description = "demo training dataset":
   *        fv.createTrainTestSplit(startTime, endTime, description);
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @return List of dataframe of features and labels
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats.
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data can be
   * recreated by calling `featureView.getTrainingData` with the metadata created.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String startTime = "20220101000000";
   *        String endTime = "20220630235959";
   *        String description = "demo training dataset":
   *        Long seed = ...;
   *        StatisticsConfig statisticsConfig = ...;
   *        Map<String, String> readOptions = ...;
   *        FilterLogic extraFilterLogic = ...,
   *        Filter extraFilter = ...;
   *        fv.trainValidationTestSplit(startTime, endTime,  description, seed, statisticsConfig, readOptions,
   *        extraFilterLogic, extraFilter);
   * }
   * </pre>
   *
   * @param startTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param endTime Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param readOptions Additional read options as key/value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return List of dataframe of features and labels
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage.  The training data is split
   * into train and test set at random or according to time ranges. The training data can be recreated by calling
   * `featureView.getTrainTestSplit` with the metadata created.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String testStart = "20220701000000";
   *        String testEnd = "20220830235959";
   *        String description = "demo training dataset":
   *        StatisticsConfig statisticsConfig = ...;
   *        Map<String, String> readOptions = ...;
   *        FilterLogic extraFilterLogic = ...,
   *        Filter extraFilter = ...;
   *        fv.trainValidationTestSplit(null, trainStart, trainEnd, testStart, trainEnd, description);
   *        // or random split
   *        fv.trainValidationTestSplit(30, null, null, null, null, description);
   * }
   * </pre>
   *
   * @param testSize size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @return List of Spark Dataframes containing training dataset splits.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data is split into
   * train and test set at random or according to time ranges. The training data can be recreated by calling
   * `feature_view.getTrainTestSplit` with the metadata created.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String testStart = "20220701000000";
   *        String testEnd = "20220830235959";
   *        String description = "demo training dataset":
   *        Long seed = ...;
   *        StatisticsConfig statisticsConfig = ...;
   *        Map<String, String> readOptions = ...;
   *        FilterLogic extraFilterLogic = ...,
   *        Filter extraFilter = ...;
   *        fv.trainTestSplit(null, strainStart, trainEnd, testStart, trainEnd, description, seed, statisticsConfig,
   *        readOptions, extraFilterLogic, extraFilter);
   *
   *        // or random split
   *        fv.trainValidationTestSplit(30, null, null, null, null, description, seed, statisticsConfig, readOptions,
   *        extraFilterLogic, extraFilter);
   * }
   * </pre>
   *
   * @param testSize size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param readOptions Additional read options as key/value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return List of Spark Dataframes containing training dataset splits.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data is split into
   * train, validation, and test set at random or according to time ranges. The training data can be recreated by
   * calling `feature_view.getTrainValidationTestSplit` with the metadata created.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String validationStart = "20220701000000";
   *        String validationEnd = "20220830235959";
   *        String testStart = "20220901000000";
   *        String testEnd = "20220931235959";
   *        String description = "demo training dataset":
   *        fv.trainValidationTestSplit(null, null, trainStart, trainEnd, validationStart, validationEnd, testStart,
   *        testEnd,  description);
   *
   *        //or based on random split
   *        fv.trainValidationTestSplit(20, 10, null, null, null, null, null, null, description);
   * }
   * </pre>
   *
   * @param validationSize size of validation set.
   * @param testSize size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                        `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                      `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @return List of Spark Dataframes containing training dataset splits.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
   * This returns the training data in memory and does not materialise data in storage. The training data is split into
   * train, validation, and test set at random or according to time ranges. The training data can be recreated by
   * calling `feature_view.get_train_validation_test_split` with the metadata created.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        // create training dataset based on time split
   *        String trainStart = "20220101000000";
   *        String trainEnd = "20220630235959";
   *        String validationStart = "20220701000000";
   *        String validationEnd = "20220830235959";
   *        String testStart = "20220901000000";
   *        String testEnd = "20220931235959";
   *        String description = "demo training dataset":
   *        Long seed = ...;
   *        StatisticsConfig statisticsConfig = ...;
   *        Map<String, String> readOptions = ...;
   *        FilterLogic extraFilterLogic = ...,
   *        Filter extraFilter = ...;
   *        fv.trainValidationTestSplit(null, null, trainStart, trainEnd, validationStart, validationEnd, testStart,
   *        testEnd,  description, seed, statisticsConfig,
   *        readOptions, extraFilterLogic, extraFilter);
   *
   *        //or based on random split
   *        fv.trainValidationTestSplit(20, 10, null, null, null, null, null, null, description, statisticsConfig,
   *        seed, readOptions, extraFilterLogic, extraFilter);
   * }
   * </pre>
   *
   * @param validationSize size of validation set.
   * @param testSize size of test set.
   * @param trainStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                   `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param trainEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                 `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                        `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param validationEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                      `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testStart Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                  `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param testEnd Datetime string. The String should be formatted in one of the following formats `yyyyMMdd`,
   *                `yyyyMMddHH`, `yyyyMMddHHmm`, or `yyyyMMddHHmmss`.
   * @param description A string describing the contents of the training dataset to  improve discoverability for
   *                    Data Scientists
   * @param seed Define a seed to create the random splits with, in order to guarantee reproducability,
   * @param statisticsConfig  A configuration object, to generally enable descriptive statistics computation for
   *                          this feature group, `"correlations`" to turn on feature correlation  computation,
   *                          `"histograms"` to compute feature value frequencies and `"exact_uniqueness"` to compute
   *                          uniqueness, distinctness and entropy. The values should be booleans indicating the
   *                          setting. To fully turn off statistics computation pass `statisticsConfig=null`.
   * @param readOptions Additional read options as key/value pairs.
   * @param extraFilterLogic Additional filters (set of Filter objects) to be attached to the training dataset.
   *                         The filters will be also applied in `getBatchData`.
   * @param extraFilter  Additional filter to be attached to the training dataset. The filter will be also applied
   *                     in `getBatchData`.
   * @return List of Spark Dataframes containing training dataset splits.
   * @throws FeatureStoreException If Client is not connected to Hopsworks and/or unable to identify format of the
   *                               provided date strings to date formats;
   * @throws IOException Generic IO exception.
   * @throws ParseException In case it's unable to parse provided date strings to date types.
   */
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

  /**
   * Delete a training dataset (data only).
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Delete a training dataset version 1
   *        fv.purgeAllTrainingData(1);
   * }
   * </pre>
   *
   * @param version Version of the training dataset to be removed.
   * @throws FeatureStoreException If Client is not connected to Hopsworks;
   * @throws IOException Generic IO exception.
   */
  public void purgeTrainingData(Integer version) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingDatasetOnly(this, version);
  }

  /**
   * Delete all training datasets in this feature view (data only).
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Delete a training dataset.
   *        fv.purgeAllTrainingData(1);
   * }
   * </pre>
   *
   * @throws FeatureStoreException If Client is not connected to Hopsworks;
   * @throws IOException Generic IO exception.
   */
  public void purgeAllTrainingData() throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingDatasetOnly(this);
  }

  /**
   * Delete a training dataset. This will delete both metadata and training data.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Delete a training dataset version 1.
   *        fv.deleteTrainingDataset(1);
   * }
   * </pre>
   *
   * @param version Version of the training dataset to be removed.
   * @throws FeatureStoreException If Client is not connected to Hopsworks;
   * @throws IOException Generic IO exception.
   */
  public void deleteTrainingDataset(Integer version) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingData(this, version);
  }

  /**
   * Delete all training datasets. This will delete both metadata and training data.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //Delete all training datasets in this feature view.
   *        fv.deleteAllTrainingDatasets();
   * }
   * </pre>
   *
   * @throws FeatureStoreException If Client is not connected to Hopsworks;
   * @throws IOException Generic IO exception.
   */
  public void deleteAllTrainingDatasets() throws FeatureStoreException, IOException {
    featureViewEngine.deleteTrainingData(this);
  }

  /**
   * Add name/value tag to the training dataset.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //add tag to datasets version 1 in this feature view.
   *        JSONObject json = ...;
   *        fv.addTrainingDatasetTag(1, "tag_name", json);
   * }
   * </pre>
   *
   * @param version training dataset version
   * @param name name of the tag
   * @param value value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void addTrainingDatasetTag(Integer version, String name, Object value) throws FeatureStoreException,
      IOException {
    featureViewEngine.addTag(this, name, value, version);
  }

  /**
   * Get all tags of the training dataset.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //get tags of training dataset version 1 in this feature view.
   *        fv.getTrainingDatasetTags(1);
   * }
   * </pre>
   *
   * @param version training dataset version
   * @return a map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  @JsonIgnore
  public Map<String, Object> getTrainingDatasetTags(Integer version) throws FeatureStoreException, IOException {
    return featureViewEngine.getTags(this, version);
  }

  /**
   * Get a single tag value of the training dataset.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //get tag with name `"demo_name"` of training dataset version 1 in this feature view.
   *        fv.getTrainingDatasetTags(1, "demo_name");
   * }
   * </pre>
   *
   * @param version training dataset version
   * @param name name of the tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  @JsonIgnore
  public Object getTrainingDatasetTag(Integer version, String name) throws FeatureStoreException, IOException {
    return featureViewEngine.getTag(this, name, version);
  }

  /**
   * Delete a tag of the training dataset.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = ...;
   *        //get feature view handle
   *        FeatureView fv = ...;
   *        //delete tag with name `"demo_name"` of training dataset version 1 in this feature view.
   *        fv.deleteTrainingDatasetTag(1, "demo_name");
   * }
   * </pre>
   *
   * @param version Tag version.
   * @param name Name of the tag to be deleted.
   * @throws FeatureStoreException If Client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   */
  public void deleteTrainingDatasetTag(Integer version, String name) throws FeatureStoreException, IOException {
    featureViewEngine.deleteTag(this, name, version);
  }

  /**
   * Get set of primary key names that is used as keys in input dict object for `getServingVector` method.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = ...;
   *        // get feature view handle
   *        FeatureView fv = ...;
   *        // get set of primary key names
   *        fv.getPrimaryKeys();
   * }
   * </pre>
   *
   * @return Set of serving keys
   * @throws FeatureStoreException In case client is not connected to Hopsworks.
   * @throws IOException Generic IO exception.
   * @throws SQLException In case there is online storage (RonDB) access error or other errors.
   * @throws ClassNotFoundException In case class `com.mysql.jdbc.Driver` can not be found.
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
