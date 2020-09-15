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
import org.apache.parquet.Strings;

@AllArgsConstructor
@NoArgsConstructor
public class Feature {
  @Getter @Setter
  private String name;

  @Getter @Setter
  private String type;

  @Getter @Setter
  private String onlineType;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private Boolean primary;

  @Getter @Setter
  private Boolean partition;

  @Getter @Setter
  private Boolean precombine;

  public Feature(String name) {
    this.name = name;
  }

  public Feature(String name, String type) {
    this.name = name;
    this.type = type;
  }

  @Builder
  public Feature(String name, String type, String onlineType, Boolean primary, Boolean partition, Boolean precombine)
      throws FeatureStoreException {
    if (Strings.isNullOrEmpty(name)) {
      throw new FeatureStoreException("Name is required when creating a feature");
    }
    this.name = name;

    if (Strings.isNullOrEmpty(type)) {
      throw new FeatureStoreException("Type is required when creating a feature");
    }
    this.type = type;
    this.onlineType = onlineType;
    this.primary = primary;
    this.partition = partition;
    this.precombine = precombine;
  }
}
