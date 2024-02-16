/*
 *  Copyright (c) 2020-2023. Hopsworks AB
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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.engine.FeatureGroupEngineBase;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.metadata.Statistics;
import com.logicalclocks.hsfs.metadata.Subject;
import com.logicalclocks.hsfs.metadata.User;

import lombok.Getter;
import lombok.Setter;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public abstract class FeatureGroupBase<T> {

  @Getter
  @Setter
  protected Integer id;

  @Getter
  @Setter
  @JsonIgnore
  protected FeatureStoreBase featureStore;

  @Getter
  @Setter
  protected String type = "featuregroupDTO";

  @Getter
  @Setter
  protected String name;

  @Getter
  @Setter
  protected Integer version;

  @Getter
  @Setter
  protected String description;

  @JsonIgnore
  protected List<String> primaryKeys;

  @Getter
  @Setter
  protected List<Feature> features;

  @Getter
  @Setter
  protected String eventTime;

  @Getter
  protected Date created;

  @Getter
  protected User creator;

  @Getter
  @Setter
  protected StatisticsConfig statisticsConfig = new StatisticsConfig();

  @Getter
  @Setter
  protected List<String> expectationsNames;

  @Getter
  @Setter
  protected String location;

  @Getter
  @Setter
  protected TimeTravelFormat timeTravelFormat = TimeTravelFormat.HUDI;

  @Getter
  @Setter
  protected Boolean onlineEnabled;

  @Getter
  @Setter
  protected String onlineTopicName;

  @Getter
  @Setter
  protected String topicName;

  @Getter
  @Setter
  protected String notificationTopicName;

  @Getter
  @Setter
  protected List<String> statisticColumns;

  @Setter
  protected DeltaStreamerJobConf deltaStreamerJobConf;

  @Getter
  protected Boolean deprecated;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  protected List<String> partitionKeys;

  @JsonIgnore
  // This is only used in the client. In the server they are aggregated in the `features` field
  protected String hudiPrecombineKey;

  @JsonIgnore
  protected Subject subject;

  protected FeatureGroupEngineBase featureGroupEngineBase = new FeatureGroupEngineBase();
  protected FeatureGroupUtils utils = new FeatureGroupUtils();

  protected static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupBase.class);

  public void setDeprecated(Boolean deprecated) {
    this.deprecated = deprecated;
    checkDeprecated();
  }

  public void checkDeprecated() {
    if (Boolean.TRUE.equals(this.deprecated)) {
      LOGGER.warn(String.format("Feature Group `%s`, version `%s` is deprecated", this.name, this.version));
    }
  }

  public void delete() throws FeatureStoreException, IOException {
    LOGGER.warn("JobWarning: All jobs associated to feature group `" + name + "`, version `"
        + version + "` will be removed.");
    featureGroupEngineBase.delete(this);
  }

  /**
   * Add name/value tag to the feature group.
   *
   * @param name  name of the tag
   * @param value value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
    featureGroupEngineBase.addTag(this, name, value);
  }

  /**
   * Get all tags of the feature group.
   *
   * @return map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  @JsonIgnore
  public Map<String, Object> getTags() throws FeatureStoreException, IOException {
    return featureGroupEngineBase.getTags(this);
  }

  /**
   * Get a single tag value of the feature group.
   *
   * @param name name of tha tag
   * @return The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  @JsonIgnore
  public Object getTag(String name) throws FeatureStoreException, IOException {
    return featureGroupEngineBase.getTag(this, name);
  }

  /**
   * Delete a tag of the feature group.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    featureGroupEngineBase.deleteTag(this, name);
  }

  /**
   * Update the description of the feature group.
   *
   * @param description feature group description.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateDescription(String description) throws FeatureStoreException, IOException {
    featureGroupEngineBase.updateDescription(this, description, this.getClass());
  }

  /**
   * Update the notification topic name of the feature group.
   *
   * @param notificationTopicName feature group notification topic name.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateNotificationTopicName(String notificationTopicName) throws FeatureStoreException, IOException {
    featureGroupEngineBase.updateNotificationTopicName(this, notificationTopicName, this.getClass());
  }

  /**
   * Deprecate the feature group.
   *
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateDeprecated() throws FeatureStoreException, IOException {
    updateDeprecated(true);
  }

  /**
   * Deprecate the feature group.
   *
   * @param deprecate identifies if feature group should be deprecated.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateDeprecated(Boolean deprecate) throws FeatureStoreException, IOException {
    featureGroupEngineBase.updateDeprecated(this, deprecate, this.getClass());
  }

  /**
   * Update the description of a single feature.
   *
   * @param featureName Name of the feature
   * @param description Description of the feature
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateFeatureDescription(String featureName, String description)
      throws FeatureStoreException, IOException {

    Feature feature = this.getFeature(featureName);

    Feature newFeature = new Feature(feature.getName(), feature.getType(), feature.getOnlineType(),
            feature.getPrimary(), feature.getPartition(), feature.getDefaultValue(), description);

    featureGroupEngineBase.updateFeatures(this,
        Collections.singletonList(newFeature),
        this.getClass());
  }

  public abstract T read() throws FeatureStoreException, IOException;

  public abstract T read(boolean online) throws FeatureStoreException, IOException;

  public abstract T read(Map<String, String> readOptions) throws FeatureStoreException, IOException;

  public abstract T read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException;

  public abstract T read(String wallclockTime) throws FeatureStoreException, IOException, ParseException;

  public abstract T read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException;

  public abstract QueryBase asOf(String wallclockTime) throws FeatureStoreException, ParseException;

  public abstract QueryBase asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException,
      ParseException;

  public abstract void show(int numRows) throws FeatureStoreException, IOException;

  public abstract void show(int numRows, boolean online) throws FeatureStoreException, IOException;

  public abstract void insert(T featureData) throws IOException, FeatureStoreException, ParseException;

  public abstract void insert(T featureData,  Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException;

  public abstract void insert(T featureData, Storage storage)
      throws IOException, FeatureStoreException, ParseException;

  public abstract void insert(T featureData, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException;

  public abstract void insert(T featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException;

  public abstract void insert(T featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException;

  public abstract void insert(T featureData, HudiOperationType operation)
      throws FeatureStoreException, IOException, ParseException;

  public abstract void insert(T featureData, Storage storage, boolean overwrite, HudiOperationType operation,
                     Map<String, String> writeOptions) throws FeatureStoreException, IOException, ParseException;

  public abstract void insert(T  featureData, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException;

  public abstract void insert(T featureData, boolean overwrite, Map<String, String> writeOptions,
                              JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException;

  public abstract void commitDeleteRecord(T featureData)
      throws FeatureStoreException, IOException, ParseException;

  public abstract void commitDeleteRecord(T featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException;

  public abstract Map<Long, Map<String, String>> commitDetails()
      throws IOException, FeatureStoreException, ParseException;

  public abstract Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException;

  public abstract Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException;

  public abstract QueryBase selectFeatures(List<Feature> features);

  public abstract QueryBase select(List<String> features);

  public abstract QueryBase selectAll();

  public abstract QueryBase selectExceptFeatures(List<Feature> features);

  public abstract QueryBase selectExcept(List<String> features);

  public abstract Object insertStream(T featureData) throws Exception;

  public abstract Object insertStream(T featureData, String queryName) throws Exception;

  public abstract Object insertStream(T featureData, Map<String, String> writeOptions) throws Exception;

  public abstract Object insertStream(T featureData, String queryName, Map<String, String> writeOptions)
      throws Exception;

  public abstract Object insertStream(T featureData, String queryName, String outputMode) throws Exception;

  public abstract Object insertStream(T featureData, String queryName, String outputMode,
                                     String checkpointLocation) throws Exception;

  public abstract Object insertStream(T featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout) throws Exception;

  public abstract Object insertStream(T featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout, String checkpointLocation)
      throws Exception;

  public abstract Object insertStream(T featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout,  String checkpointLocation,
                                     Map<String, String> writeOptions) throws Exception;

  public abstract Object insertStream(T featureData, String queryName, String outputMode, boolean awaitTermination,
                                      String checkpointLocation) throws Exception;

  public abstract Object insertStream(T featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout,  String checkpointLocation,
                                     Map<String, String> writeOptions, JobConfiguration jobConfiguration)
      throws Exception;


  /**
   * Update the metadata of multiple features.
   * Currently only feature description updates are supported.
   *
   * @param features List of Feature metadata objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public abstract void updateFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException;

  /**
   * Update the metadata of multiple features.
   * Currently only feature description updates are supported.
   *
   * @param feature Feature metadata object
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public abstract void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException;

  /**
   * Append features to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features list of Feature metadata objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public abstract void appendFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException;

  /**
   * Append a single feature to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features List of Feature metadata objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public abstract void appendFeatures(Feature features) throws FeatureStoreException, IOException, ParseException;

  /**
   * Update the statistics configuration of the feature group.
   * Change the `enabled`, `histograms`, `correlations` or `columns` attributes and persist
   * the changes by calling this method.
   *
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateStatisticsConfig() throws FeatureStoreException, IOException {
    featureGroupEngineBase.updateStatisticsConfig(this, this.getClass());
  }

  /**
   * Recompute the statistics for the feature group and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public abstract Statistics computeStatistics() throws FeatureStoreException, IOException, ParseException;

  public abstract Statistics computeStatistics(String wallclockTime) throws FeatureStoreException, IOException,
      ParseException;

  /**
   * Get the last statistics commit for the feature group.
   *
   * @return statistics object of latest commit
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  @JsonIgnore
  public abstract Statistics getStatistics() throws FeatureStoreException, IOException;

  @JsonIgnore
  public Subject getSubject() throws FeatureStoreException, IOException {
    if (subject == null) {
      subject = utils.getSubject(this);
    }
    return subject;
  }

  @JsonIgnore
  public void unloadSubject() {
    this.subject = null;
  }

  /**
   * Retrieve a feature of the feature group by name.
   *
   * @param name feature name
   * @return Feature metadata object
   * @throws FeatureStoreException FeatureStoreException
   */
  @JsonIgnore
  public Feature getFeature(String name) throws FeatureStoreException {
    return features.stream().filter(f -> f.getName().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new FeatureStoreException("Feature with name `" + name
            + "` not found in feature group `" + this.name + "`."));
  }

  @JsonIgnore
  public List<String> getPrimaryKeys() {
    if (primaryKeys == null) {
      primaryKeys = features.stream().filter(f -> f.getPrimary()).map(Feature::getName).collect(Collectors.toList());
    }
    return primaryKeys;
  }

  @JsonIgnore
  public List<String> getComplexFeatures() {
    return utils.getComplexFeatures(features);
  }

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    return getSubject().getSchema();
  }

  @JsonIgnore
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    return utils.getFeatureAvroSchema(featureName, utils.getDeserializedAvroSchema(getAvroSchema()));
  }

  @JsonIgnore
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getEncodedAvroSchema(getDeserializedAvroSchema(), utils.getComplexFeatures(features));
  }

  @JsonIgnore
  public Schema getDeserializedEncodedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getDeserializedEncodedAvroSchema(getDeserializedAvroSchema(), utils.getComplexFeatures(features));
  }

  @JsonIgnore
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getDeserializedAvroSchema(getAvroSchema());
  }


}
