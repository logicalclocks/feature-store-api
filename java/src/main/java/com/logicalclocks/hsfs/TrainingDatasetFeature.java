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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class TrainingDatasetFeature {
  @Getter
  @Setter
  private String name;

  @Getter
  @Setter
  private String type;

  @Getter
  @Setter
  private FeatureGroup featureGroup;

  @Getter
  @Setter
  private Integer index;

  @Getter
  @Setter
  private Boolean label = false;

  @Builder
  public TrainingDatasetFeature(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public TrainingDatasetFeature(String name, String type, Integer index) {
    this.name = name;
    this.type = type;
    this.index = index;
  }

  public TrainingDatasetFeature(String name, String type, Integer index, Boolean label) {
    this.name = name;
    this.type = type;
    this.index = index;
    this.label = label;
  }
}
