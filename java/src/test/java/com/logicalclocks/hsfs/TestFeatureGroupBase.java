/*
 * Copyright (c) 2022 Hopsworks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs;

import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestFeatureGroupBase {

  @Test
  public void testSelect_fgId_set() {
    FeatureGroupBase featureGroupBase = new FeatureGroupBase(null, 1);
    List<String> selectFeatures = Arrays.asList("ft1", "ft2");

    Query result = featureGroupBase.select(selectFeatures);

    Assertions.assertEquals(1, result.getLeftFeatures().get(0).getFeatureGroupId());
    Assertions.assertEquals(1, result.getLeftFeatures().get(1).getFeatureGroupId());
  }
}
