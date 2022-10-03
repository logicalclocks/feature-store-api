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

import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;


public class TestStorageConnector {

  @Test
  public void testBigQueryCredentialsBase64Encoded(@TempDir Path tempDir) throws IOException {
    // Arrange
    String credentials = "{\"type\": \"service_account\", \"project_id\": \"test\"}";
    Path credentialsFile = tempDir.resolve("bigquery.json");
    Files.write(credentialsFile, credentials.getBytes());

    StorageConnector.BigqueryConnector bigqueryConnector = new StorageConnector.BigqueryConnector();
    bigqueryConnector.setKeyPath("file://" + credentialsFile);

    // Act
    Map<String, String> sparkOptions = bigqueryConnector.sparkOptions();

    // Assert
    Assertions.assertEquals(credentials,
      new String(Base64.getDecoder().decode(sparkOptions.get(Constants.BIGQ_CREDENTIALS)), StandardCharsets.UTF_8));
  }

  @Test
  public void testSnowflakeConnector_read() throws Exception {
    // Arrange
    StorageConnector.SnowflakeConnector snowflakeConnector = new StorageConnector.SnowflakeConnector();
    snowflakeConnector.setTable(Constants.SNOWFLAKE_TABLE);

    SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
    SparkEngine.setInstance(sparkEngine);

    ArgumentCaptor<Map> mapArg = ArgumentCaptor.forClass(Map.class);
    String query = "select * from dbtable";

    // Act
    snowflakeConnector.read(query, null, null, null);
    Mockito.verify(sparkEngine).read(Mockito.any(), Mockito.any(), mapArg.capture(), Mockito.any());

    // Assert
    Assertions.assertFalse(mapArg.getValue().containsKey(Constants.SNOWFLAKE_TABLE));
    Assertions.assertEquals(query, mapArg.getValue().get("query"));
  }
}
