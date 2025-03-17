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

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;

import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

@NoArgsConstructor
public class Query extends QueryBase<Query, StreamFeatureGroup, DataStream<?>> {

  public Query(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    super(leftFeatureGroup, leftFeatures);
  }
}
