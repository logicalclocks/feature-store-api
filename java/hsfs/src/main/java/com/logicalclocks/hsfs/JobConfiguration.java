/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class JobConfiguration {

  @Getter
  @Setter
  private String type = "sparkJobConfiguration";

  @Getter
  @Setter
  private int amMemory = 2048;

  @Getter
  @Setter
  private int amVCores = 1;

  @Getter
  @Setter
  @JsonProperty("spark.executor.instances")
  private int executorInstances;

  @Getter
  @Setter
  @JsonProperty("spark.executor.cores")
  private int executorCores = 1;

  @Getter
  @Setter
  @JsonProperty("spark.executor.memory")
  private int executorMemory = 4096;

  @Getter
  @Setter
  @JsonProperty("spark.dynamicAllocation.enabled")
  private boolean dynamicAllocationEnabled = true;

  @Getter
  @Setter
  @JsonProperty("spark.dynamicAllocation.minExecutors")
  private int dynamicAllocationMinExecutors = 1;

  @Getter
  @Setter
  @JsonProperty("spark.dynamicAllocation.maxExecutors")
  private int dynamicAllocationMaxExecutors = 2;

  @Getter
  @Setter
  @JsonProperty("spark.dynamicAllocation.initialExecutors")
  private int dynamicAllocationInitialExecutors = 1;

  public JobConfiguration(int amMemory, int amVCores, int executorCores, int executorMemory,
                          boolean dynamicAllocationEnabled, int dynamicAllocationMaxExecutors,
                          int dynamicAllocationMinExecutors) {
    this.amMemory = amMemory;
    this.amVCores = amVCores;
    this.executorCores = executorCores;
    this.executorMemory = executorMemory;
    this.dynamicAllocationEnabled = dynamicAllocationEnabled;
    this.dynamicAllocationMaxExecutors = dynamicAllocationMaxExecutors;
    this.dynamicAllocationMinExecutors = dynamicAllocationMinExecutors;
  }
}
