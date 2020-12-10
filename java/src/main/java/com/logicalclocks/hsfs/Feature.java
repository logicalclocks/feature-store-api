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

import com.logicalclocks.hsfs.constructor.Filter;
import com.logicalclocks.hsfs.constructor.SqlFilterCondition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.apache.parquet.Strings;

@AllArgsConstructor
@NoArgsConstructor
public class Feature {
  @Getter
  @Setter
  private String name;

  @Getter
  @Setter
  private String type;

  @Getter
  @Setter
  private String onlineType;

  @Getter
  @Setter
  private String description;

  @Getter
  @Setter
  private Boolean primary;

  @Getter
  @Setter
  private Boolean partition;

  @Getter
  @Setter
  private String defaultValue;

  @Getter
  @Setter
  private Integer featureGroupId;

  public Feature(@NonNull String name) {
    this.name = name;
  }

  public Feature(@NonNull String name, @NonNull FeatureGroup featureGroup) {
    this.name = name;
    this.featureGroupId = featureGroup.getId();
  }

  public Feature(@NonNull String name, @NonNull String type) {
    this.name = name;
    this.type = type;
  }

  public Feature(@NonNull String name, @NonNull String type, @NonNull String defaultValue) {
    this.name = name;
    this.type = type;
    this.defaultValue = defaultValue;
  }

  public Feature(String name, String type, Boolean primary, Boolean partition)
      throws FeatureStoreException {
    if (Strings.isNullOrEmpty(name)) {
      throw new FeatureStoreException("Name is required when creating a feature");
    }
    this.name = name;

    if (Strings.isNullOrEmpty(type)) {
      throw new FeatureStoreException("Type is required when creating a feature");
    }
    this.type = type;
    this.primary = primary;
    this.partition = partition;
  }

  @Builder
  public Feature(String name, String type, String onlineType, Boolean primary, Boolean partition, String defaultValue)
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
    this.defaultValue = defaultValue;
  }

  public Filter lt(String value) {
    return new Filter(this, SqlFilterCondition.LESS_THAN, value);
  }

  public Filter lt(Integer value) {
    return new Filter(this, SqlFilterCondition.LESS_THAN, value.toString());
  }

  public Filter lt(Float value) {
    return new Filter(this, SqlFilterCondition.LESS_THAN, value.toString());
  }

  public Filter le(String value) {
    return new Filter(this, SqlFilterCondition.LESS_THAN_OR_EQUAL, value);
  }

  public Filter le(Integer value) {
    return new Filter(this, SqlFilterCondition.LESS_THAN_OR_EQUAL, value.toString());
  }

  public Filter le(Float value) {
    return new Filter(this, SqlFilterCondition.LESS_THAN_OR_EQUAL, value.toString());
  }

  public Filter eq(String value) {
    return new Filter(this, SqlFilterCondition.EQUALS, value);
  }

  public Filter eq(Integer value) {
    return new Filter(this, SqlFilterCondition.EQUALS, value.toString());
  }

  public Filter eq(Float value) {
    return new Filter(this, SqlFilterCondition.EQUALS, value.toString());
  }

  public Filter ne(String value) {
    return new Filter(this, SqlFilterCondition.NOT_EQUALS, value);
  }

  public Filter ne(Integer value) {
    return new Filter(this, SqlFilterCondition.NOT_EQUALS, value.toString());
  }

  public Filter ne(Float value) {
    return new Filter(this, SqlFilterCondition.NOT_EQUALS, value.toString());
  }

  public Filter gt(String value) {
    return new Filter(this, SqlFilterCondition.GREATER_THAN, value);
  }

  public Filter gt(Integer value) {
    return new Filter(this, SqlFilterCondition.GREATER_THAN, value.toString());
  }

  public Filter gt(Float value) {
    return new Filter(this, SqlFilterCondition.GREATER_THAN, value.toString());
  }

  public Filter ge(String value) {
    return new Filter(this, SqlFilterCondition.GREATER_THAN_OR_EQUAL, value);
  }

  public Filter ge(Integer value) {
    return new Filter(this, SqlFilterCondition.GREATER_THAN_OR_EQUAL, value.toString());
  }

  public Filter ge(Float value) {
    return new Filter(this, SqlFilterCondition.GREATER_THAN_OR_EQUAL, value.toString());
  }
}
