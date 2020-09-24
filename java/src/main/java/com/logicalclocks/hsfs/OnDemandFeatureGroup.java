/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.engine.OnDemandFeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupInternal;
import com.logicalclocks.hsfs.metadata.Query;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OnDemandFeatureGroup extends FeatureGroupInternal {

  @Getter @Setter
  private String name;

  @Getter @Setter
  private Integer version;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private List<Feature> features;

  @Getter @Setter
  private StorageConnector storageConnector;

  @Getter
  private Date created;

  @Getter
  private String creator;

  @Getter @Setter
  private String query;

  @Getter @Setter
  private String type = "onDemandFeaturegroupDTO";

  private OnDemandFeatureGroupEngine onDemandFeatureGroupEngine = new OnDemandFeatureGroupEngine();

  @Builder
  public OnDemandFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String query,
                              @NonNull StorageConnector storageConnector, String description, List<Feature> features) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.query = query;
    this.description = description;
    this.storageConnector = storageConnector;
    this.features = features;
  }

  public OnDemandFeatureGroup() {
  }

  public void save() throws FeatureStoreException, IOException {
    onDemandFeatureGroupEngine.saveFeatureGroup(this);
  }

  public Query selectFeatures(List<Feature> features) throws FeatureStoreException, IOException {
    return new Query(this, features);
  }

  public Query selectAll() throws FeatureStoreException, IOException {
    return new Query(this, getFeatures());
  }

  public Query select(List<String> features) throws FeatureStoreException, IOException {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList  = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return selectAll().read();
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    read().show(numRows);
  }

  public void delete() throws FeatureStoreException, IOException {
    onDemandFeatureGroupEngine.delete(this);
  }

  /**
   * Add a tag without value to the feature group.
   *
   * @param name name of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name) throws FeatureStoreException, IOException {
    addTag(name, null);
  }

  /**
   * Add name/value tag to the feature group.
   *
   * @param name name of the tag
   * @param value value of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void addTag(String name, String value) throws FeatureStoreException, IOException {
    onDemandFeatureGroupEngine.addTag(this, name, value);
  }

  /**
   * Get all tags of the feature group.
   *
   * @return map of all tags from name to value
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, String> getTag() throws FeatureStoreException, IOException {
    return getTag(null);
  }

  /**
   * Get a single tag value of the feature group.
   *
   * @param name name of tha tag
   * @return string value of the tag
   * @throws FeatureStoreException
   * @throws IOException
   */
  @JsonIgnore
  public Map<String, String> getTag(String name) throws FeatureStoreException, IOException {
    return onDemandFeatureGroupEngine.getTag(this, name);
  }

  /**
   * Delete a tag of the feature group.
   *
   * @param name name of the tag to be deleted
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    onDemandFeatureGroupEngine.deleteTag(this, name);
  }
}
