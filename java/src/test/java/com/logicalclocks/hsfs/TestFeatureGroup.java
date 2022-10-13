/*
 *  Copyright (c) 2022. Logical Clocks AB
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

import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.engine.StreamFeatureGroupEngine;
import com.logicalclocks.hsfs.util.Constants;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestFeatureGroup {

  @Test
  public void testFeatureGroup() throws IOException, FeatureStoreException, ParseException {
    // Arrange
    FeatureStore fs = Mockito.mock(FeatureStore.class);
    /*
    FeatureGroup aa = Mockito.verify(fs).createFeatureGroup()
        .name("marketing")
        .version(1)
        .description("Features about inbound/outbound communication with customers")
        .onlineEnabled(true)
        .statisticsConfig(new StatisticsConfig(false, false, false, false))
        .primaryKeys(Collections.singletonList("cc_num"))
        .eventTime("ts")
        .build();
    */

    StreamFeatureGroupEngine streamFeatureGroupEngine = Mockito.mock(StreamFeatureGroupEngine.class);

    // Act
    StreamFeatureGroup featureGroup = Mockito.mock(StreamFeatureGroup.class);


    List<Feature> features = new ArrayList<>();
    features.add( new Feature("a"));
    features.add( new Feature("b"));
    features.add( new Feature("c"));
    featureGroup.setName("cardTransactions10mAgg");
    featureGroup.setVersion(1);
    featureGroup.setFeatures(features);

    // Assert
    Exception exception = assertThrows(FeatureStoreException.class, () -> {
      streamFeatureGroupEngine.saveFeatureGroupMetaData(featureGroup, Collections.singletonList("cc_num"), null, null, null, null);;
    });

  }
}
