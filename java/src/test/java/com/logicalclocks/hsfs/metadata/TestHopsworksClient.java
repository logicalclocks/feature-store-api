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

package com.logicalclocks.hsfs.metadata;

import org.apache.commons.io.IOUtils;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestHopsworksClient {

  @Test
  public void testStringEntitySerialization() throws IOException {
    // Arrange
    HopsworksClient hopsworksClient = new HopsworksClient(null, null);
    User user = new User();
    user.setEmail("test@test.com");
    user.setFirstName("test");
    user.setLastName("de la Rúa Martínez");

    // Act
    StringEntity stringEntity = hopsworksClient.buildStringEntity(user);

    // Assert
    Assertions.assertEquals("Content-Type: application/json; charset=UTF-8",
        stringEntity.getContentType().toString());

    String json = IOUtils.toString(stringEntity.getContent(), StandardCharsets.UTF_8);
    Assertions.assertEquals("{\"email\":\"test@test.com\",\"firstName\":\"test\",\"lastName\":\"de la Rúa Martínez\"}",
        json);
  }
}
