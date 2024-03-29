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

package com.logicalclocks.hsfs.constructor;

import com.logicalclocks.hsfs.Feature;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@NoArgsConstructor
public class Join<T extends QueryBase> {

  @Getter
  @Setter
  private T query;

  @Getter
  @Setter
  private List<Feature> on;
  @Getter
  @Setter
  private List<Feature> leftOn;
  @Getter
  @Setter
  private List<Feature> rightOn;

  @Getter
  @Setter
  private JoinType type;

  @Getter
  @Setter
  private String prefix;

  public Join(T query, JoinType type, String prefix) {
    this.query = query;
    this.type = type;
    this.prefix = prefix;
  }

  public Join(T query, List<Feature> on, JoinType type, String prefix) {
    this.query = query;
    this.on = on;
    this.type = type;
    this.prefix = prefix;
  }

  public Join(T query, List<Feature> leftOn, List<Feature> rightOn, JoinType type, String prefix) {
    this.query = query;
    this.leftOn = leftOn;
    this.rightOn = rightOn;
    this.type = type;
    this.prefix = prefix;
  }
}
