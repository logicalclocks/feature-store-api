/*
 *  Copyright (c) 2022. Logical Clocks AB
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

package com.logicalclocks.hsfs.generic;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.logicalclocks.hsfs.generic.constructor.Filter;
import com.logicalclocks.hsfs.generic.constructor.FilterLogic;
import com.logicalclocks.hsfs.generic.constructor.Query;
import com.logicalclocks.hsfs.generic.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.generic.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.generic.engine.VectorServer;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public abstract class FeatureView {

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

    /* TODO (davit)
    public FeatureView build() throws FeatureStoreException, IOException {
      FeatureView featureView = new FeatureView(name, version, query, description, featureStore, labels);
      featureViewEngine.save(featureView);
      return featureView;
    }
    */
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
  public abstract void getBatchData();

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

  public abstract Integer createTrainingData(String startTime, String endTime, String description,
                                             DataFormat dataFormat,
                                             Boolean coalesce, StorageConnector storageConnector, String location,
                                             Long seed, StatisticsConfig statisticsConfig,
                                             Map<String, String> writeOptions,
                                             FilterLogic extraFilterLogic, Filter extraFilter)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Integer createTrainTestSplit(Float testSize, String trainStart, String trainEnd, String testStart,
                                               String testEnd, String description, DataFormat dataFormat)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Integer createTrainTestSplit(Float testSize, String trainStart, String trainEnd, String testStart,
                                               String testEnd, String description, DataFormat dataFormat,
                                               Boolean coalesce, StorageConnector storageConnector, String location,
                                               Long seed, StatisticsConfig statisticsConfig,
                                               Map<String, String> writeOptions, FilterLogic extraFilterLogic,
                                               Filter extraFilter)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Integer createTrainValidationTestSplit(
      Float validationSize, Float testSize, String trainStart, String trainEnd, String validationStart,
      String validationEnd, String testStart, String testEnd, String description, DataFormat dataFormat
  ) throws IOException, FeatureStoreException, ParseException;

  public abstract Integer createTrainValidationTestSplit(Float validationSize, Float testSize, String trainStart,
                                                    String trainEnd, String validationStart, String validationEnd,
                                                    String testStart, String testEnd, String description,
                                                    DataFormat dataFormat, Boolean coalesce,
                                                    StorageConnector storageConnector, String location, Long seed,
                                                    StatisticsConfig statisticsConfig, Map<String, String> writeOptions,
                                                    FilterLogic extraFilterLogic, Filter extraFilter)
      throws IOException, FeatureStoreException, ParseException;

  public abstract void recreateTrainingDataset(Integer version, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException;


  public abstract Object getTrainingData(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Object getTrainTestSplit(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Object getTrainValidationTestSplit(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Object trainingData(String startTime, String endTime, String description,
                                    Long seed, StatisticsConfig statisticsConfig, Map<String, String> readOptions,
                                    FilterLogic extraFilterLogic, Filter extraFilter)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Object trainTestSplit(Float testSize, String trainStart, String trainEnd, String testStart,
                                              String testEnd, String description, Long seed,
                                              StatisticsConfig statisticsConfig, Map<String, String> readOptions,
                                              FilterLogic extraFilterLogic, Filter extraFilter)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Object trainValidationTestSplit(Float validationSize, Float testSize, String trainStart,
                                                String trainEnd, String validationStart, String validationEnd,
                                                String testStart, String testEnd, String description, Long seed,
                                                StatisticsConfig statisticsConfig, Map<String, String> readOptions,
                                                FilterLogic extraFilterLogic, Filter extraFilter)
      throws IOException, FeatureStoreException, ParseException;


  public abstract void purgeTrainingData(Integer version) throws FeatureStoreException, IOException;

  public abstract void purgeAllTrainingData() throws FeatureStoreException, IOException;

  public abstract void deleteTrainingDataset(Integer version) throws FeatureStoreException, IOException;

  public abstract void deleteAllTrainingDatasets() throws FeatureStoreException, IOException;
}
