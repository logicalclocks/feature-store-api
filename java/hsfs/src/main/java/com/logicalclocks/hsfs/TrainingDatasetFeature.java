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
import com.logicalclocks.hsfs.util.Constants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class TrainingDatasetFeature {
  @Getter
  private String name;

  @Getter
  @Setter
  private String type;

  @Getter
  @Setter
  private StreamFeatureGroup featuregroup;

  @Getter
  @Setter
  private Integer index;

  @Getter
  @Setter
  private Boolean label = false;

  @Getter
  @Setter
  private TransformationFunction transformationFunction;

  @Builder
  public TrainingDatasetFeature(String name, String type) {
    setName(name);
    this.type = type;
  }

  public TrainingDatasetFeature(String name, String type, Integer index) {
    setName(name);
    this.type = type;
    this.index = index;
  }

  public TrainingDatasetFeature(String name, String type, Integer index, Boolean label) {
    setName(name);
    this.type = type;
    this.index = index;
    this.label = label;
  }

  public TrainingDatasetFeature(StreamFeatureGroup featuregroup, String name, Boolean label) {
    setName(name);
    this.featuregroup = featuregroup;
    this.label = label;
  }

  public void setName(String name) {
    this.name = name.toLowerCase();
  }

  @JsonIgnore
  public boolean isComplex() {
    return !label && Constants.COMPLEX_FEATURE_TYPES.stream().anyMatch(c -> type.toUpperCase().startsWith(c));
  }
}
