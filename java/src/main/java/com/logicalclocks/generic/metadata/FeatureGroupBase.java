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

package com.logicalclocks.generic.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.logicalclocks.generic.constructor.Filter;
import com.logicalclocks.generic.constructor.FilterLogic;
import com.logicalclocks.generic.constructor.Query;
import com.logicalclocks.generic.DeltaStreamerJobConf;
import com.logicalclocks.generic.Feature;
import com.logicalclocks.generic.FeatureStoreBase;
import com.logicalclocks.generic.FeatureStoreException;
import com.logicalclocks.generic.StatisticsConfig;
import com.logicalclocks.generic.TimeTravelFormat;
import com.logicalclocks.generic.engine.FeatureGroupBaseEngine;
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
public abstract class FeatureGroupBase {

  @Getter
  @Setter
  protected Integer id;

  @Getter
  @Setter
  protected FeatureStoreBase featureStoreBase;

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

  private FeatureGroupBaseEngine featureGroupBaseEngine = new FeatureGroupBaseEngine();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupBase.class);

  public FeatureGroupBase(FeatureStoreBase featureStoreBase, Integer id) {
    this.featureStoreBase = featureStoreBase;
    this.id = id;
  }

  public abstract Query selectFeatures(List<Feature> features);

  public Query select(List<String> features) {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  public abstract Query selectAll();

  public abstract Query selectExceptFeatures(List<Feature> features);

  public abstract Query selectExcept(List<String> features);

  public void delete() throws FeatureStoreException, IOException {
    featureGroupBaseEngine.delete(this);
  }

  public abstract Object read(boolean online, Map<String, String> readOptions) throws FeatureStoreException,
      IOException;

  /**
   * Add name/value tag to the feature group.
   *
   * @param name  name of the tag
   * @param value value of the tag. The value of a tag can be any valid json - primitives, arrays or json objects.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.addTag(this, name, value);
  }

  /**
   * Get all tags of the feature group.
   *
   * @return map of tag name and values. The value of a tag can be any valid json - primitives, arrays or json objects
   * @throws FeatureStoreException
   * @throws IOException
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
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Object getTag(String name) throws FeatureStoreException, IOException {
    return featureGroupBaseEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the feature group.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.deleteTag(this, name);
  }

  /**
   * Update the description of the feature group.
   *
   * @param description
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void updateDescription(String description) throws FeatureStoreException, IOException {
    featureGroupBaseEngine.updateDescription(this, description, this.getClass());
  }

  /**
   * Update the description of a single feature.
   *
   * @param featureName
   * @param description
   * @throws FeatureStoreException
   * @throws IOException
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
   * @param features
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract void updateFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException;

  /**
   * Update the metadata of multiple features.
   * Currently only feature description updates are supported.
   *
   * @param feature
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException;

  /**
   * Append features to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract void appendFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException;

  /**
   * Append a single feature to the schema of the feature group.
   * It is only possible to append features to a feature group. Removing features is considered a breaking change.
   *
   * @param features
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract void appendFeatures(Feature features) throws FeatureStoreException, IOException, ParseException;

  /**
   * Update the statistics configuration of the feature group.
   * Change the `enabled`, `histograms`, `correlations` or `columns` attributes and persist
   * the changes by calling this method.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void updateStatisticsConfig() throws FeatureStoreException, IOException {
    featureGroupBaseEngine.updateStatisticsConfig(this, this.getClass());
  }

  /**
   * Recompute the statistics for the feature group and save them to the feature store.
   *
   * @return statistics object of computed statistics
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract <T> T computeStatistics() throws FeatureStoreException, IOException;

  /**
   * Get the last statistics commit for the feature group.
   *
   * @return statistics object of latest commit
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public <T> T getStatistics() throws FeatureStoreException, IOException {
    return null;
  }

  /**
   * Filter the query based on a condition for a feature or a conjunction of multiple filters.
   *
   * @param filter
   * @return Query
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Query filter(Filter filter) throws FeatureStoreException, IOException {
    return this.selectAll().genericFilter(filter);
  }

  /**
   * Filter the query based on a condition for a feature or a conjunction of multiple filters.
   *
   * @param filter
   * @return Query
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Query filter(FilterLogic filter) throws FeatureStoreException, IOException {
    return this.selectAll().genericFilter(filter);
  }

  /**
   * Retrieve a feature of the feature group by name.
   *
   * @param name
   * @return Feature
   * @throws FeatureStoreException
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

  public abstract String getOnlineTopicName() throws FeatureStoreException, IOException;

  public abstract void setDeltaStreamerJobConf(DeltaStreamerJobConf deltaStreamerJobConf)
      throws FeatureStoreException, IOException;

  @JsonIgnore
  public abstract List<String> getComplexFeatures();

  @JsonIgnore
  public abstract String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException;

  @JsonIgnore
  public abstract String getEncodedAvroSchema() throws FeatureStoreException, IOException;

  @JsonIgnore
  public abstract Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException;

  public abstract TimeTravelFormat getTimeTravelFormat();
}
