/*
 *  Copyright (c) 2020-2022. Hopsworks AB
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

package com.logicalclocks.base.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.logicalclocks.base.constructor.Filter;
import com.logicalclocks.base.constructor.FilterLogic;
import com.logicalclocks.base.constructor.QueryBase;
import com.logicalclocks.base.DeltaStreamerJobConf;
import com.logicalclocks.base.Feature;
import com.logicalclocks.base.FeatureStoreBase;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.engine.FeatureGroupBaseEngine;
import com.logicalclocks.base.engine.FeatureGroupUtils;

import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.StatisticsConfig;

import lombok.Getter;
import lombok.NoArgsConstructor;
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

@NoArgsConstructor
public class FeatureGroupBase {

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
  protected String onlineTopicName;

  @JsonIgnore
  protected Subject subject;

  protected FeatureGroupBaseEngine featureGroupBaseEngine = new FeatureGroupBaseEngine();
  protected FeatureGroupUtils utils = new FeatureGroupUtils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupBase.class);

  public FeatureGroupBase(FeatureStoreBase featureStore, Integer id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  public QueryBase selectFeatures(List<Feature> features) {
    return null;
  }

  public QueryBase select(List<String> features) {
    return null;
  }

  public QueryBase selectAll() {
    return null;
  }

  public QueryBase selectExceptFeatures(List<Feature> features) {
    return null;
  }

  public QueryBase selectExcept(List<String> features) {
    return null;
  }

  public void delete() throws FeatureStoreException, IOException {
    LOGGER.warn("JobWarning: All jobs associated to feature group `" + name + "`, version `"
        + version + "` will be removed.");
    featureGroupBaseEngine.delete(this);
  }

  public Object read(boolean online, Map<String, String> readOptions) throws FeatureStoreException,
      IOException {
    return null;
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
    featureGroupBaseEngine.addTag(this, name, value);
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
    return featureGroupBaseEngine.getTags(this);
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
    return featureGroupBaseEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the feature group.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.deleteTag(this, name);
  }

  /**
   * Update the description of the feature group.
   *
   * @param description feature group description.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateDescription(String description) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.updateDescription(this, description, this.getClass());
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
    featureGroupBaseEngine.updateFeatures(this,
        Collections.singletonList(Feature.builder().name(featureName).description(description).type("tmp").build()),
        this.getClass());
  }

  /**
   * Update the metadata of multiple features.
   * Currently only feature description updates are supported.
   *
   * @param features List of Feature metadata objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public void updateFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {

  }

  /**
   * Update the metadata of multiple features.
   * Currently only feature description updates are supported.
   *
   * @param feature Feature metadata object
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {

  }

  /**
   * Append features to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features list of Feature metadata objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public void appendFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {

  }

  /**
   * Append a single feature to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features List of Feature metadata objects
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   * @throws ParseException ParseException
   */
  public void appendFeatures(Feature features) throws FeatureStoreException, IOException, ParseException {

  }

  /**
   * Update the statistics configuration of the feature group.
   * Change the `enabled`, `histograms`, `correlations` or `columns` attributes and persist
   * the changes by calling this method.
   *
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public void updateStatisticsConfig() throws FeatureStoreException, IOException {
    featureGroupBaseEngine.updateStatisticsConfig(this, this.getClass());
  }

  /**
   * Recompute the statistics for the feature group and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public <T> T computeStatistics() throws FeatureStoreException, IOException {
    return null;
  }

  /**
   * Get the last statistics commit for the feature group.
   *
   * @return statistics object of latest commit
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  @JsonIgnore
  public <T> T getStatistics() throws FeatureStoreException, IOException {
    return null;
  }

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
   * Filter the query based on a condition for a feature or a conjunction of multiple filters.
   *
   * @param filter Filter metadata object
   * @return Query object
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public QueryBase filter(Filter filter) throws FeatureStoreException, IOException {
    return this.selectAll().genericFilter(filter);
  }

  /**
   * Filter the query based on a condition for a feature or a conjunction of multiple filters.
   *
   * @param filter Filter metadata object
   * @return Query object
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
   */
  public QueryBase filter(FilterLogic filter) throws FeatureStoreException, IOException {
    return this.selectAll().genericFilter(filter);
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

  public void setDeltaStreamerJobConf(DeltaStreamerJobConf deltaStreamerJobConf)
      throws FeatureStoreException, IOException {
  }

  @JsonIgnore
  public List<String> getComplexFeatures() {
    return null;
  }

  @JsonIgnore
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    return null;
  }

  @JsonIgnore
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    return null;
  }

  @JsonIgnore
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    return null;
  }

  public TimeTravelFormat getTimeTravelFormat() {
    return null;
  }
}
