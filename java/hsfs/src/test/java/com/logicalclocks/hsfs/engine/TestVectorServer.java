/*
 * Copyright (c) 2025 Hopsworks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.constructor.PreparedStatementParameter;
import com.logicalclocks.hsfs.constructor.ServingPreparedStatement;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestVectorServer {

  @Test
  public void testQueryOrder() throws Exception {
    VectorServer vectorServer = Mockito.mock(VectorServer.class);
    Mockito.doCallRealMethod().when(vectorServer).initPreparedStatement(Mockito.any(), Mockito.any(),
        Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean());
    Mockito.when(vectorServer.getOrderedServingPreparedStatements()).thenCallRealMethod();
    Mockito.when(vectorServer.getPreparedStatementParameters()).thenCallRealMethod();

    FeatureStoreBase featureStoreBase = Mockito.mock(FeatureStoreBase.class);

    List<ServingPreparedStatement> servingPreparedStatements = new ArrayList<>();
    servingPreparedStatements.add(
        new ServingPreparedStatement(1,
            Collections.singletonList(new PreparedStatementParameter("id", 0)),
            "SELECT * FROM table_1 WHERE id=?"));
    servingPreparedStatements.add(
        new ServingPreparedStatement(0,
            Arrays.asList(new PreparedStatementParameter("id", 1),
                new PreparedStatementParameter("pk", 0)),
            "SELECT * FROM table_0 WHERE pk=? AND id=?"));

    vectorServer.initPreparedStatement(featureStoreBase, null, servingPreparedStatements, false, true);

    Map<Integer, ServingPreparedStatement> queries = vectorServer.getOrderedServingPreparedStatements();
    Assert.assertEquals("SELECT * FROM table_0 WHERE pk=? AND id=?", queries.get(0).getQueryOnline());
    Assert.assertEquals("SELECT * FROM table_1 WHERE id=?", queries.get(1).getQueryOnline());

    TreeMap<String, Integer> preparedStatementParameters = vectorServer.getPreparedStatementParameters().get(0);
    Assert.assertEquals((Integer)0, preparedStatementParameters.get("pk"));
    Assert.assertEquals((Integer)1, preparedStatementParameters.get("id"));
  }

  @Test
  public void testAllKeys() throws Exception {
    VectorServer vectorServer = Mockito.mock(VectorServer.class);
    Mockito.doCallRealMethod().when(vectorServer).setServingKeys(Mockito.any());
    Mockito.doCallRealMethod().when(vectorServer).getFeatureVector(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

    vectorServer.setServingKeys((HashSet<String>) Stream.of("id", "pk").collect(Collectors.toSet()));
    Map<String, Object> entries = new HashMap<>();
    entries.put("pk", 1);
    entries.put("id", 1);

    vectorServer.getFeatureVector(null, entries, false);
  }

  @Test
  public void testMissingKeys() throws Exception {
    VectorServer vectorServer = Mockito.mock(VectorServer.class);
    Mockito.doCallRealMethod().when(vectorServer).setServingKeys(Mockito.any());
    Mockito.doCallRealMethod().when(vectorServer).getFeatureVector(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

    vectorServer.setServingKeys((HashSet<String>) Stream.of("id", "pk").collect(Collectors.toSet()));
    Map<String, Object> entries = new HashMap<>();
    entries.put("id", 1);

    assertThrows(IllegalArgumentException.class, () -> vectorServer.getFeatureVector(null, entries, false));
  }

}