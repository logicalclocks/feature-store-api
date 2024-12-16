/*
 *  Copyright (c) 2024. Hopsworks AB
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
package com.logicalclocks.hsfs.flink.engine;

import com.logicalclocks.hsfs.*;
import com.logicalclocks.hsfs.flink.FeatureStore;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import org.apache.kafka.common.config.SslConfigs;
import org.bouncycastle.cert.X509CertificateHolder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.bouncycastle.openssl.PEMParser;

import java.io.IOException;
import java.io.StringReader;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

public class TestFlinkEngine {

    @Test
    public void testKafkaProperties_Certificates() throws IOException, FeatureStoreException, CertificateException {
        // Arrange
        HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
        hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));

        StorageConnector.KafkaConnector kafkaConnector = new StorageConnector.KafkaConnector();
        kafkaConnector.setSslKeystoreLocation(this.getClass().getResource("/test_kstore.jks").getPath());
        kafkaConnector.setSslKeystorePassword("O74K016I5UTB7YYPC6K6RXIM9F7LVPFW23FNK8WF3JEOO7Y607VCU7E7691UQ3CA");
        kafkaConnector.setSslTruststoreLocation(this.getClass().getResource("/test_tstore.jks").getPath());
        kafkaConnector.setSslTruststorePassword("O74K016I5UTB7YYPC6K6RXIM9F7LVPFW23FNK8WF3JEOO7Y607VCU7E7691UQ3CA");
        kafkaConnector.setSecurityProtocol(SecurityProtocol.SSL);
        kafkaConnector.setSslEndpointIdentificationAlgorithm(SslEndpointIdentificationAlgorithm.EMPTY);
        kafkaConnector.setExternalKafka(true);

        StorageConnectorApi storageConnectorApi = Mockito.mock(StorageConnectorApi.class);
        Mockito.when(storageConnectorApi.getKafkaStorageConnector(Mockito.any(), Mockito.anyBoolean()))
                .thenReturn(kafkaConnector);
        FlinkEngine flinkEngine = FlinkEngine.getInstance();
        flinkEngine.setStorageConnectorApi(storageConnectorApi);

        StreamFeatureGroup featureGroup = new StreamFeatureGroup();
        featureGroup.setFeatureStore(new FeatureStore());

        // Act
        Map<String, String> kafkaOptions = flinkEngine.getKafkaConfig(featureGroup, new HashMap<>());

        // Assert
        Assert.assertEquals("PEM", kafkaOptions.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
        Assert.assertEquals("PEM", kafkaOptions.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));

        String keystoreChainPem = kafkaOptions.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG);
        String trustStorePem = kafkaOptions.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG);

        try (PEMParser pemParser = new PEMParser(new StringReader(keystoreChainPem))) {
            Assert.assertEquals("CN=FraudWorkshop__fabio000",
                    ((X509CertificateHolder) pemParser.readObject()).getSubject().toString());

            Assert.assertEquals("C=SE,O=Hopsworks,OU=core,CN=HopsRootCA",
                    ((X509CertificateHolder) pemParser.readObject()).getIssuer().toString());
        }

        try (PEMParser pemParser = new PEMParser(new StringReader(trustStorePem))) {
            Assert.assertEquals("C=SE,O=Hopsworks,OU=core,CN=HopsRootCA",
                    ((X509CertificateHolder) pemParser.readObject()).getSubject().toString());
        }
    }
}
