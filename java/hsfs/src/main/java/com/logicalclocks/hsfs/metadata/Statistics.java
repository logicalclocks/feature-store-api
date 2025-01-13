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

package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collection;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Statistics extends RestDto<Statistics> {
  private Long commitTime;
  private Float rowPercentage;
  private Collection<FeatureDescriptiveStatistics> featureDescriptiveStatistics;
  // Feature group
  private Integer featureGroupId;
  // Feature view
  private String featureViewName;
  private Integer featureViewVersion;
  // Both feature group and feature view
  private Long windowStartTime;
  @JsonAlias("featureGroupCommitId")
  private Long windowEndTime;
  // Training dataset
  private Integer trainingDatasetId;
  private List<SplitStatistics> splitStatistics;
  private Boolean forTransformation;

  public Statistics(Long commitTime, Float rowPercentage,
      Collection<FeatureDescriptiveStatistics> featureDescriptiveStatistics, Long windowEndTime, Long windowStartTime) {
    this.commitTime = commitTime;
    this.rowPercentage = rowPercentage;
    this.featureDescriptiveStatistics = featureDescriptiveStatistics;
    this.windowEndTime = windowEndTime;
    this.windowStartTime = windowStartTime;
  }

  public Statistics(Long commitTime, Float rowPercentage, List<SplitStatistics> splitStatistics) {
    this.commitTime = commitTime;
    this.rowPercentage = rowPercentage;
    this.splitStatistics = splitStatistics;
  }
}
