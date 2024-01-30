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

package com.logicalclocks.hsfs.spark;

import com.logicalclocks.hsfs.ExternalDataFormat;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.spark.engine.FeatureGroupEngine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestExternalFeatureGroup {

  @Test
  public void testExternalFeatureGroupPrimaryKey() throws FeatureStoreException, IOException {

    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    StorageConnector storageConnector = Mockito.mock(StorageConnector.class);
    FeatureGroupEngine externalFeatureGroupEngine = new FeatureGroupEngine();

    // Act
    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    ExternalFeatureGroup externalFeatureGroup = new ExternalFeatureGroup(featureStore, "fgName", 1,
        "SELECT *", ExternalDataFormat.HUDI, "path", null, storageConnector, "description",
        Collections.singletonList("primaryKey"), features, null, "featureA", false, "topic", null, null);

    Exception pkException = assertThrows(FeatureStoreException.class, () -> {
      externalFeatureGroupEngine.saveExternalFeatureGroup(externalFeatureGroup);
    });

    // Assert
    Assertions.assertEquals(pkException.getMessage(),
        "Provided primary key(s) primarykey doesn't exist in feature dataframe");
  }

  @Test
  public void testFeatureGroupEventTimeFeature() throws FeatureStoreException, IOException {

    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    StorageConnector storageConnector = Mockito.mock(StorageConnector.class);
    FeatureGroupEngine externalFeatureGroupEngine = new FeatureGroupEngine();

    // Act
    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    ExternalFeatureGroup externalFeatureGroup = new ExternalFeatureGroup(featureStore, "fgName", 1,
        "SELECT *", ExternalDataFormat.HUDI, "path", null, storageConnector, "description",
        Collections.singletonList("featureA"), features, null, "eventTime", false, "topic", null, null);

    Exception pkException = assertThrows(FeatureStoreException.class, () -> {
      externalFeatureGroupEngine.saveExternalFeatureGroup(externalFeatureGroup);
    });

    // Assert
    Assertions.assertEquals(pkException.getMessage(),
        "Provided eventTime feature name eventTime doesn't exist in feature dataframe");
  }
}
