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

package com.logicalclocks.hsfs.spark.constructor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.constructor.FeatureGroupAlias;
import com.logicalclocks.hsfs.constructor.FsQueryBase;
import com.logicalclocks.hsfs.spark.ExternalFeatureGroup;
import com.logicalclocks.hsfs.spark.StreamFeatureGroup;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
public class FsQuery extends FsQueryBase<StreamFeatureGroup, ExternalFeatureGroup> {
  @Override
  public void registerOnDemandFeatureGroups() throws FeatureStoreException, IOException {
    if (getOnDemandFeatureGroups() == null || getOnDemandFeatureGroups().isEmpty()) {
      return;
    }

    for (FeatureGroupAlias externalFeatureGroupAlias : getOnDemandFeatureGroups()) {
      String alias = externalFeatureGroupAlias.getAlias();
      ExternalFeatureGroup onDemandFeatureGroup =
          (ExternalFeatureGroup) externalFeatureGroupAlias.getOnDemandFeatureGroup();

      SparkEngine.getInstance().registerOnDemandTemporaryTable(onDemandFeatureGroup, alias);
    }
  }

  @Override
  public void registerHudiFeatureGroups(Map<String, String> readOptions) throws FeatureStoreException {
    for (FeatureGroupAlias featureGroupAlias : getHudiCachedFeatureGroups()) {
      SparkEngine.getInstance().registerHudiTemporaryTable(featureGroupAlias, readOptions);
    }
  }
}
