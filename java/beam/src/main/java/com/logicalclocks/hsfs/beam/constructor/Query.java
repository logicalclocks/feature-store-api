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

package com.logicalclocks.hsfs.beam.constructor;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.constructor.QueryBase;

import lombok.NoArgsConstructor;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

@NoArgsConstructor
public class Query extends QueryBase<Query, StreamFeatureGroup, PCollection<Object>> {

  public Query(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    super(leftFeatureGroup, leftFeatures);
  }

}
