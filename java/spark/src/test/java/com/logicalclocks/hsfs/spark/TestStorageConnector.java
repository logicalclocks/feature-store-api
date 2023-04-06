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

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.util.Constants;

import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;

import com.logicalclocks.hsfs.spark.util.StorageConnectorUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.parquet.Strings;
import org.apache.spark.SparkContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    if (SystemUtils.IS_OS_WINDOWS) {
      bigqueryConnector.setKeyPath("file:///" + credentialsFile.toString().replace( "\\", "/" ));
    } else {
      bigqueryConnector.setKeyPath("file://" + credentialsFile);
    }

    // Act
    // Base64 encode the credentials file
    String localKeyPath = SparkEngine.getInstance().addFile(bigqueryConnector.getKeyPath());
    String fileContent = Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(localKeyPath)));

    // Assert
    Assertions.assertEquals(credentials,
      new String(Base64.getDecoder().decode(fileContent), StandardCharsets.UTF_8));
  }

  @Test
  public void testSnowflakeConnector_read() throws Exception {
    // Arrange
    StorageConnector.SnowflakeConnector snowflakeConnector = new StorageConnector.SnowflakeConnector();
    snowflakeConnector.setTable(Constants.SNOWFLAKE_TABLE);

    SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
    SparkEngine.setInstance(sparkEngine);
    StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
    ArgumentCaptor<Map> mapArg = ArgumentCaptor.forClass(Map.class);
    String query = "select * from dbtable";

    // Act
    storageConnectorUtils.read(snowflakeConnector, query);
    Mockito.verify(sparkEngine).read(Mockito.any(), Mockito.any(), mapArg.capture(), Mockito.any());

    // Assert
    Assertions.assertFalse(mapArg.getValue().containsKey(Constants.SNOWFLAKE_TABLE));
    Assertions.assertEquals(query, mapArg.getValue().get("query"));
    // clear static instance
    SparkEngine.setInstance(null);
  }

  @Test
  public void testGcsConnectorCredentials(@TempDir Path tempDir) throws IOException, FeatureStoreException {
    // Arrange
    String credentials = "{\"type\": \"service_account\", \"project_id\": \"test\", \"private_key_id\": \"123456\", "+
      "\"private_key\": \"-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----\", " +
      "\"client_email\": \"test@project.iam.gserviceaccount.com\"}";
    Path credentialsFile = tempDir.resolve("gcs.json");
    Files.write(credentialsFile, credentials.getBytes());

    StorageConnector.GcsConnector gcsConnector = new StorageConnector.GcsConnector();
    if (SystemUtils.IS_OS_WINDOWS) {
      gcsConnector.setKeyPath("file:///" + credentialsFile.toString().replace( "\\", "/" ));
    } else {
      gcsConnector.setKeyPath("file://" + credentialsFile);
    }
    gcsConnector.setStorageConnectorType(StorageConnectorType.GCS);

    // Act
    SparkEngine.getInstance().setupConnectorHadoopConf(gcsConnector);
    SparkContext sc = SparkEngine.getInstance().getSparkSession().sparkContext();
    // Assert
    Assertions.assertEquals(
      "test@project.iam.gserviceaccount.com",
      sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_EMAIL));
    Assertions.assertEquals(
      "123456",sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_KEY_ID));
    Assertions.assertEquals("-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----",sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_KEY));
    Assertions.assertEquals(Constants.PROPERTY_GCS_FS_VALUE,sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_FS_KEY));
    Assertions.assertEquals("true",sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_ENABLE));
    Assertions.assertTrue(Strings.isNullOrEmpty(sc.hadoopConfiguration().get(Constants.PROPERTY_ENCRYPTION_KEY)));
    Assertions.assertTrue(Strings.isNullOrEmpty(sc.hadoopConfiguration().get(Constants.PROPERTY_ENCRYPTION_HASH)));
    Assertions.assertTrue(Strings.isNullOrEmpty(sc.hadoopConfiguration().get(Constants.PROPERTY_ALGORITHM)));
  }

  @Test
  public void testGcsConnectorCredentials_encrypted(@TempDir Path tempDir) throws IOException,
    FeatureStoreException {
    // Arrange
    String credentials = "{\"type\": \"service_account\", \"project_id\": \"test\", \"private_key_id\": \"123456\", "+
      "\"private_key\": \"-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----\", " +
      "\"client_email\": \"test@project.iam.gserviceaccount.com\"}";
    Path credentialsFile = tempDir.resolve("gcs.json");
    Files.write(credentialsFile, credentials.getBytes());

    StorageConnector.GcsConnector gcsConnector = new StorageConnector.GcsConnector();
    if (SystemUtils.IS_OS_WINDOWS) {
      gcsConnector.setKeyPath("file:///" + credentialsFile.toString().replace( "\\", "/" ));
    } else {
      gcsConnector.setKeyPath("file://" + credentialsFile);
    }
    gcsConnector.setStorageConnectorType(StorageConnectorType.GCS);
    gcsConnector.setAlgorithm("AES256");
    gcsConnector.setEncryptionKey("encryptionkey");
    gcsConnector.setEncryptionKeyHash("encryptionkeyhash");

    // Act
    SparkEngine.getInstance().setupConnectorHadoopConf(gcsConnector);
    SparkContext sc = SparkEngine.getInstance().getSparkSession().sparkContext();

    // Assert
    Assertions.assertEquals("test@project.iam.gserviceaccount.com",sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_EMAIL));
    Assertions.assertEquals("123456",sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_KEY_ID));
    Assertions.assertEquals("-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----",sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_KEY));
    Assertions.assertEquals(Constants.PROPERTY_GCS_FS_VALUE,sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_FS_KEY));
    Assertions.assertEquals("true",sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_ENABLE));
    Assertions.assertEquals("encryptionkey",sc.hadoopConfiguration().get(Constants.PROPERTY_ENCRYPTION_KEY));
    Assertions.assertEquals("encryptionkeyhash",sc.hadoopConfiguration().get(Constants.PROPERTY_ENCRYPTION_HASH));
    Assertions.assertEquals("AES256",sc.hadoopConfiguration().get(Constants.PROPERTY_ALGORITHM));
  }

}
