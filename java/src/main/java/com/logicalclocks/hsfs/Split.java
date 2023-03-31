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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

@AllArgsConstructor
@NoArgsConstructor
public class Split {

  public static final String TRAIN = "train";
  public static final String VALIDATION = "validataion";
  public static final String TEST = "test";

  public enum SplitType {
    RANDOM_SPLIT,
    TIME_SERIES_SPLIT;
  }

  public Split(String name, Float percentage) {
    this.name = name;
    this.percentage = percentage;
    this.splitType = SplitType.RANDOM_SPLIT;
  }

  public Split(String name, Date startTime, Date endTime) {
    this.name = name;
    this.startTime = startTime;
    this.endTime = endTime;
    this.splitType = SplitType.TIME_SERIES_SPLIT;
  }

  @Getter
  @Setter
  private SplitType splitType;

  @Getter
  @Setter
  private String name;

  @Getter
  @Setter
  private Float percentage;

  @Getter
  @Setter
  private Date startTime;

  @Getter
  @Setter
  private Date endTime;
}
