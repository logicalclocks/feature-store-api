/*
 *  Copyright (c) 2023. Hopsworks AB
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

package com.logicalclocks.hsfs.flink.constructor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.constructor.FsQueryBase;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import lombok.AllArgsConstructor;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
public class FsQuery extends FsQueryBase<StreamFeatureGroup, StreamFeatureGroup> {
  @Override
  public void registerOnDemandFeatureGroups() {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void registerHudiFeatureGroups(Map<String, String> readOptions) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }
}
