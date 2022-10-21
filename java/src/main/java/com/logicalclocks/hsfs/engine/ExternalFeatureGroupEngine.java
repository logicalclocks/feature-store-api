/*
 *  Copyright (c) 2020-2022. Hopsworks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.generic.FeatureStoreException;
import com.logicalclocks.generic.engine.FeatureGroupBaseEngine;
import com.logicalclocks.generic.metadata.FeatureGroupApi;
import com.logicalclocks.generic.metadata.HopsworksClient;
import com.logicalclocks.hsfs.ExternalFeatureGroup;
import org.apache.http.entity.StringEntity;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class ExternalFeatureGroupEngine extends FeatureGroupBaseEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();

  public ExternalFeatureGroup saveFeatureGroup(ExternalFeatureGroup externalFeatureGroup)
      throws FeatureStoreException, IOException {
    Dataset<Row> onDemandDataset = null;
    if (externalFeatureGroup.getFeatures() == null) {
      onDemandDataset = SparkEngine.getInstance()
          .registerOnDemandTemporaryTable(externalFeatureGroup, "read_ondmd");
      externalFeatureGroup.setFeatures(SparkEngine.getInstance().parseFeatureGroupSchema(onDemandDataset,
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

    ExternalFeatureGroup apiFg = save(externalFeatureGroup);
    externalFeatureGroup.setId(apiFg.getId());

    return externalFeatureGroup;
  }

  private ExternalFeatureGroup save(ExternalFeatureGroup externalFeatureGroup)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String featureGroupJson = hopsworksClient.getObjectMapper().writeValueAsString(externalFeatureGroup);

    return (ExternalFeatureGroup)
        featureGroupApi.saveInternal(externalFeatureGroup, new StringEntity(featureGroupJson),
        ExternalFeatureGroup.class);
  }
}
