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

package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.engine.FeatureGroupBaseEngine;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class FeatureGroupBase {

  @Getter @Setter
  protected Integer id;

  @Getter @Setter
  protected FeatureStore featureStore;

  @Getter @Setter
  protected TimeTravelFormat timeTravelFormat;

  @Getter @Setter
  protected String name;

  @Getter @Setter
  protected Integer version;

  @Getter @Setter
  protected String description;

  @Getter @Setter
  protected List<Feature> features;

  @Getter
  protected Date created;

  @Getter
  protected String creator;

  private FeatureGroupBaseEngine featureGroupBaseEngine = new FeatureGroupBaseEngine();

  public Query selectFeatures(List<Feature> features) {
    return new Query(this, features);
  }

  public Query selectAll() {
    return new Query(this, getFeatures());
  }

  public Query select(List<String> features) {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList  = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  public void delete() throws FeatureStoreException, IOException {
    featureGroupBaseEngine.delete(this);
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
    featureGroupBaseEngine.addTag(this, name, value);
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
}
