/*
 *  Copyright (c) 2020-2022. Logical Clocks AB
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

package com.logicalclocks.hsfs.spark.engine;

import com.logicalclocks.hsfs.generic.FeatureStoreException;
import com.logicalclocks.hsfs.generic.ExternalFeatureGroup;
import com.logicalclocks.hsfs.generic.engine.FeatureGroupBaseEngine;
import com.logicalclocks.hsfs.generic.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupApi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class ExternalFeatureGroupEngine extends FeatureGroupBaseEngine {

  private FeatureGroupUtils utils = new FeatureGroupUtils();

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();

  public ExternalFeatureGroup saveFeatureGroup(ExternalFeatureGroup externalFeatureGroup)
      throws FeatureStoreException, IOException {
    Dataset<Row> onDemandDataset = null;
    if (externalFeatureGroup.getFeatures() == null) {
      onDemandDataset = SparkEngine.getInstance()
          .registerOnDemandTemporaryTable(externalFeatureGroup, "read_ondmd");
      externalFeatureGroup.setFeatures(utils.parseFeatureGroupSchema(onDemandDataset,
          externalFeatureGroup.getTimeTravelFormat()));
    }

    /* set primary features */
    if (externalFeatureGroup.getPrimaryKeys() != null) {
      externalFeatureGroup.getPrimaryKeys().forEach(pk ->
          externalFeatureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPrimary(true);
            }
          }));
    }

    ExternalFeatureGroup apiFg = featureGroupApi.save(externalFeatureGroup);
    externalFeatureGroup.setId(apiFg.getId());

    return externalFeatureGroup;
  }
}
