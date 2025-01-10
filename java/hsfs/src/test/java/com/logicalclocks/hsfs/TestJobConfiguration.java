/*
 *  Copyright (c) 2025. Hopsworks AB
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

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;


class TestJobConfiguration {
  @Test
  void testGetProperties() throws FeatureStoreException, IOException {
    // Arrange
    JobConfiguration jobConfiguration = new JobConfiguration(null, 1, 2, 3, 4, 0, false, 0, 0, 0, null);

    // Act
    String result = jobConfiguration.getProperties();

    // Assert
    Assert.assertEquals("spark.yarn.maxAppAttempts=2", result);
  }

  @Test
  void testGetPropertiesProvidedProperties() throws FeatureStoreException, IOException {
    // Arrange
    JobConfiguration jobConfiguration = new JobConfiguration(null, 1, 2, 3, 4, 0, false, 0, 0, 0, "spark.test=xxx");

    // Act
    String result = jobConfiguration.getProperties();

    // Assert
    Assert.assertEquals("spark.test=xxx\nspark.yarn.maxAppAttempts=2", result);
  }

  @Test
  void testGetPropertiesProvidedPropertiesOverlapDefault() throws FeatureStoreException, IOException {
    // Arrange
    JobConfiguration jobConfiguration = new JobConfiguration(null, 1, 2, 3, 4, 0, false, 0, 0, 0, "spark.yarn.maxAppAttempts=9");

    // Act
    String result = jobConfiguration.getProperties();

    // Assert
    Assert.assertEquals("spark.yarn.maxAppAttempts=9", result);
  }
}
