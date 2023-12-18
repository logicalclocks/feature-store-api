/*
 *  Copyright (c) 2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.SecurityProtocol;
import com.logicalclocks.hsfs.SslEndpointIdentificationAlgorithm;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.spark.FeatureGroup;
import com.logicalclocks.hsfs.spark.FeatureStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSparkEngine {

    @Test
    public void testConvertToDefaultDataframe() {
        // Arrange
        SparkEngine sparkEngine = SparkEngine.getInstance();

        // Act
        StructType structType = new StructType();
        structType = structType.add("A", DataTypes.StringType, false);
        structType = structType.add("B", DataTypes.StringType, false);
        structType = structType.add("C", DataTypes.StringType, false);
        structType = structType.add("D", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("value1", "value2", "value3", "value4"));

        ArrayList<String> nonNullColumns = new ArrayList<>();
        nonNullColumns.add("a");

        Dataset<Row> dfOriginal = sparkEngine.getSparkSession().createDataFrame(nums, structType);

        Dataset<Row> dfConverted = sparkEngine.convertToDefaultDataframe(dfOriginal);

        StructType expected = new StructType();
        expected = expected.add("a", DataTypes.StringType, true);
        expected = expected.add("b", DataTypes.StringType, true);
        expected = expected.add("c", DataTypes.StringType, true);
        expected = expected.add("d", DataTypes.StringType, true);
        ArrayList<StructField> expectedJava = new ArrayList<>(JavaConverters.asJavaCollection(expected.toSeq()));

        ArrayList<StructField> dfConvertedJava = new ArrayList<>(JavaConverters.asJavaCollection(dfConverted.schema().toSeq()));
        // Assert
        for (int i = 0; i < expectedJava.size(); i++) {
            Assertions.assertEquals(expectedJava.get(i), dfConvertedJava.get(i));
        }

        StructType originalExpected = new StructType();
        originalExpected = originalExpected.add("A", DataTypes.StringType, false);
        originalExpected = originalExpected.add("B", DataTypes.StringType, false);
        originalExpected = originalExpected.add("C", DataTypes.StringType, false);
        originalExpected = originalExpected.add("D", DataTypes.StringType, false);
        ArrayList<StructField> originalExpectedJava = new ArrayList<>(JavaConverters.asJavaCollection(originalExpected.toSeq()));

        ArrayList<StructField> dfOriginalJava = new ArrayList<>(JavaConverters.asJavaCollection(dfOriginal.schema().toSeq()));
        // Assert
        for (int i = 0; i < originalExpectedJava.size(); i++) {
            Assertions.assertEquals(originalExpectedJava.get(i), dfOriginalJava.get(i));
        }
    }

    @Test
    public void testGetKafkaConfig() throws FeatureStoreException, IOException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));
        System.setProperty(HopsworksInternalClient.REST_ENDPOINT_SYS, "");

        SparkEngine sparkEngine = SparkEngine.getInstance();
        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        sparkEngine.storageConnectorApi = storageConnectorApi;
        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setBootstrapServers("testServer:123");
        kafkaConnector.setOptions(Collections.singletonList(new Option("testOptionName", "testOptionValue")));
        kafkaConnector.setSslTruststorePassword("sslTruststorePassword");
        kafkaConnector.setSslKeystorePassword("sslKeystorePassword");
        kafkaConnector.setSslKeyPassword("sslKeyPassword");
        kafkaConnector.setExternalKafka(Boolean.TRUE);
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);

        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
            .thenReturn(kafkaConnector);
        ArgumentCaptor<Boolean> externalArg = ArgumentCaptor.forClass(Boolean.class);

        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("testName", "testValue");

        // Act
        Map<String, String> result = sparkEngine.getKafkaConfig(new FeatureGroup(), writeOptions);

        // Assert
        Mockito.verify(storageConnectorApi).getKafkaStorageConnector(Mockito.any(), externalArg.capture());
        Assertions.assertEquals(Boolean.FALSE, externalArg.getValue());
        Assertions.assertEquals("sslTruststorePassword", result.get("kafka.ssl.truststore.password"));
        Assertions.assertEquals("testServer:123", result.get("kafka.bootstrap.servers"));
        Assertions.assertEquals("SSL", result.get("kafka.security.protocol"));
        Assertions.assertEquals("sslKeyPassword", result.get("kafka.ssl.key.password"));
        Assertions.assertEquals("testOptionValue", result.get("kafka.testOptionName"));
        Assertions.assertEquals("", result.get("kafka.ssl.endpoint.identification.algorithm"));
        Assertions.assertEquals("sslKeystorePassword", result.get("kafka.ssl.keystore.password"));
        Assertions.assertEquals("testValue", result.get("testName"));
    }

    @Test
    public void testGetKafkaConfigExternalClient() throws FeatureStoreException, IOException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));

        SparkEngine sparkEngine = SparkEngine.getInstance();
        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        sparkEngine.storageConnectorApi = storageConnectorApi;
        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setExternalKafka(Boolean.TRUE);
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);

        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
            .thenReturn(kafkaConnector);
        ArgumentCaptor<Boolean> externalArg = ArgumentCaptor.forClass(Boolean.class);

        // Act
        sparkEngine.getKafkaConfig(new FeatureGroup(), null);

        // Assert
        Mockito.verify(storageConnectorApi).getKafkaStorageConnector(Mockito.any(), externalArg.capture());
        Assertions.assertEquals(Boolean.TRUE, externalArg.getValue());
    }

    @Test
    public void testGetKafkaConfigInternalKafka() throws FeatureStoreException, IOException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));
        System.setProperty(HopsworksInternalClient.REST_ENDPOINT_SYS, "");

        SparkEngine sparkEngine = SparkEngine.getInstance();
        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        sparkEngine.storageConnectorApi = storageConnectorApi;
        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setExternalKafka(Boolean.TRUE);
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);

        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
            .thenReturn(kafkaConnector);
        ArgumentCaptor<Boolean> externalArg = ArgumentCaptor.forClass(Boolean.class);

        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("internal_kafka", "true");

        // Act
        sparkEngine.getKafkaConfig(new FeatureGroup(), writeOptions);

        // Assert
        Mockito.verify(storageConnectorApi).getKafkaStorageConnector(Mockito.any(), externalArg.capture());
        Assertions.assertEquals(Boolean.FALSE, externalArg.getValue());
    }

    @Test
    public void testGetKafkaConfigExternalClientInternalKafka() throws FeatureStoreException, IOException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));

        SparkEngine sparkEngine = SparkEngine.getInstance();
        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        sparkEngine.storageConnectorApi = storageConnectorApi;
        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setExternalKafka(Boolean.TRUE);
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);

        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
            .thenReturn(kafkaConnector);
        ArgumentCaptor<Boolean> externalArg = ArgumentCaptor.forClass(Boolean.class);

        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("internal_kafka", "true");

        // Act
        sparkEngine.getKafkaConfig(new FeatureGroup(), writeOptions);

        // Assert
        Mockito.verify(storageConnectorApi).getKafkaStorageConnector(Mockito.any(), externalArg.capture());
        Assertions.assertEquals(Boolean.FALSE, externalArg.getValue());
    }

    @Test
    public void testMakeQueryName() {
        SparkEngine sparkEngine = SparkEngine.getInstance();
        FeatureGroup featureGroup = new FeatureGroup();
        Integer fgId = 1;
        String fgName = "test_fg";
        Integer fgVersion = 1;
        Integer projectId = 99;
        featureGroup.setId(fgId);
        featureGroup.setName(fgName);
        featureGroup.setVersion(fgVersion);
        FeatureStore featureStore = (new FeatureStore());
        featureStore.setProjectId(projectId);
        featureGroup.setFeatureStore(featureStore);
        String queryName = String.format("insert_stream_%d_%d_%s_%d_onlinefs",
                featureGroup.getFeatureStore().getProjectId(),
                featureGroup.getId(),
                featureGroup.getName(),
                featureGroup.getVersion()
        );
        // query name is null
        Assertions.assertEquals(queryName, sparkEngine.makeQueryName(null, featureGroup));
        // query name is empty
        Assertions.assertEquals(queryName, sparkEngine.makeQueryName("", featureGroup));
        // query name is not empty
        Assertions.assertEquals("test_qn", sparkEngine.makeQueryName("test_qn", featureGroup));
    }
}
