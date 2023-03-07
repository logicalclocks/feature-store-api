/*
 *  Copyright (c) 2022. Hopsworks AB
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.constructor.FsQueryBase;
import com.logicalclocks.base.constructor.HudiFeatureGroupAlias;
import com.logicalclocks.hsfs.ExternalFeatureGroup;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.SparkEngine;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
public class FsQuery extends FsQueryBase<StreamFeatureGroup> {
  @Override
  public void registerOnDemandFeatureGroups() throws FeatureStoreException, IOException {
    if (getOnDemandFeatureGroups() == null || getOnDemandFeatureGroups().isEmpty()) {
      return;
    }

    for (HudiFeatureGroupAlias externalFeatureGroupAlias : getOnDemandFeatureGroups()) {
      String alias = externalFeatureGroupAlias.getAlias();
      ExternalFeatureGroup onDemandFeatureGroup =
          (ExternalFeatureGroup) externalFeatureGroupAlias.getFeatureGroup();

      SparkEngine.getInstance().registerOnDemandTemporaryTable(onDemandFeatureGroup, alias);
    }
  }

  @Override
  public void registerHudiFeatureGroups(Map<String, String> readOptions) throws FeatureStoreException {
    for (HudiFeatureGroupAlias hudiFeatureGroupAlias : getHudiCachedFeatureGroups()) {
      SparkEngine.getInstance().registerHudiTemporaryTable(hudiFeatureGroupAlias, readOptions);
    }
  }
}
