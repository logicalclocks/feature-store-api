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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class JobConfiguration {

  private static Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
      put("spark.yarn.maxAppAttempts", "2");
  }};


  @Getter
  private String type = "sparkJobConfiguration";


  @Getter
  @Setter
  private int amMemory;

  @Getter
  @Setter
  private int amCores;

  @Getter
  @Setter
  private int executorInstances;

  @Getter
  @Setter
  private int executorCores;

  @Getter
  @Setter
  private int executorMemory;

  @Getter
  @Setter
  private boolean dynamicAllocationEnabled;

  @Getter
  @Setter
  private int dynamicAllocationMinExecutors;

  @Getter
  @Setter
  private int dynamicAllocationMaxExecutors;

  @Getter
  @Setter
  private int dynamicAllocationInitialExecutors;

  @Setter
  private String properties;

  public String getProperties() {
    // Add default properties to the properties
    for (Map.Entry<String, String> entry : JobConfiguration.DEFAULT_PROPERTIES.entrySet()) {
      String defaultProperty = entry.getKey() + "=" + entry.getValue();
      if (Strings.isNullOrEmpty(properties)) {
        properties = defaultProperty;
      } else if (!properties.contains(entry.getKey())) {
        properties = properties + "\n" + defaultProperty;
      }
    }

    return properties;
  }
}
