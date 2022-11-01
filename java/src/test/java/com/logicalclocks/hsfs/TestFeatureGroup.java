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

package com.logicalclocks.hsfs;

import com.logicalclocks.base.Feature;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestFeatureGroup {

  @Test
  public void testFeatureGroupPrimaryKey() {
    // Arrange
    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

    // Act
    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    StreamFeatureGroup featureGroup = new StreamFeatureGroup(featureStore, "fgName", 1, "description",
        Collections.singletonList("primaryKey"), Collections.singletonList("partitionKey"), "hudiPrecombineKey",
        true, features, null, "onlineTopicName", null);

    Exception pkException = assertThrows(FeatureStoreException.class, () -> {
      featureGroupEngine.saveFeatureGroupMetaData(featureGroup,
         null, null, null, null, null);;;
    });

    // Assert
    Assertions.assertEquals(pkException.getMessage(),
        "Provided primary key(s) primarykey doesn't exist in feature dataframe");
  }

  @Test
  public void testFeatureGroupEventTimeFeature() {
    // Arrange
    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    FeatureGroupEngine streamFeatureGroupEngine = new FeatureGroupEngine();

    // Act
    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    StreamFeatureGroup featureGroup = new StreamFeatureGroup(featureStore, "fgName", 1, "description",
        Collections.singletonList("featureA"), null, null,
        true, features, null, "onlineTopicName", "eventTime");

    Exception eventTimeException = assertThrows(FeatureStoreException.class, () -> {
      streamFeatureGroupEngine.saveFeatureGroupMetaData(featureGroup,
          null, null, null, null, null);;;
    });

    // Assert
    Assertions.assertEquals(eventTimeException.getMessage(),
        "Provided eventTime feature name eventTime doesn't exist in feature dataframe");
  }

  @Test
  public void testFeatureGroupPartitionPrecombineKeys() {
    // Arrange
    FeatureStore featureStore = Mockito.mock(FeatureStore.class);
    FeatureGroupEngine streamFeatureGroupEngine = new FeatureGroupEngine();

    // Act
    List<Feature> features = new ArrayList<>();
    features.add( new Feature("featureA"));
    features.add( new Feature("featureB"));
    features.add( new Feature("featureC"));

    StreamFeatureGroup featureGroup = new StreamFeatureGroup(featureStore, "fgName", 1, "description",
        Collections.singletonList("featureA"), Collections.singletonList("partitionKey"), "hudiPrecombineKey",
        true, features, null, "onlineTopicName", null);

    Exception partitionException = assertThrows(FeatureStoreException.class, () -> {
      streamFeatureGroupEngine.saveFeatureGroupMetaData(featureGroup,
          Collections.singletonList("partitionKey"), null, null, null,
          null);
    });

    Exception precombineException = assertThrows(FeatureStoreException.class, () -> {
      streamFeatureGroupEngine.saveFeatureGroupMetaData(featureGroup,
          null, "hudiPrecombineKey", null, null,
          null);
    });

    // Assert
    Assertions.assertEquals(partitionException.getMessage(),
        "Provided partition key(s) partitionKey doesn't exist in feature dataframe");

    Assertions.assertEquals(precombineException.getMessage(),
        "Provided Hudi precombine key hudiPrecombineKey doesn't exist in feature dataframe");
  }
}
