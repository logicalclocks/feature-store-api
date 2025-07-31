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

package com.logicalclocks.hsfs.flink.engine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.engine.EngineBase;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;

import com.logicalclocks.hsfs.metadata.DatasetApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksExternalClient;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.twitter.chill.Base64;
import lombok.Getter;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;
import org.apache.kafka.common.config.SslConfigs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FlinkEngine extends EngineBase {
  private static FlinkEngine INSTANCE = null;

  public static synchronized FlinkEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new FlinkEngine();
    }
    return INSTANCE;
  }

  @Getter
  private StreamExecutionEnvironment streamExecutionEnvironment;

  private FlinkEngine() {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    // Configure the streamExecutionEnvironment
    streamExecutionEnvironment.getConfig().enableObjectReuse();
  }

  public DataStreamSink<?> writeDataStream(StreamFeatureGroup streamFeatureGroup, DataStream<?> dataStream,
                                           Map<String, String> writeOptions) throws FeatureStoreException, IOException {

    DataStream<Object> genericDataStream = (DataStream<Object>) dataStream;
    Properties properties = new Properties();
    properties.putAll(getKafkaConfig(streamFeatureGroup, writeOptions));

    KafkaSink<GenericRecord> sink = KafkaSink.<GenericRecord>builder()
            .setBootstrapServers(properties.getProperty("bootstrap.servers"))
            .setKafkaProducerConfig(properties)
            .setRecordSerializer(new KafkaRecordSerializer(streamFeatureGroup))
            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    Map<String, String> complexFeatureSchemas = new HashMap<>();
    for (String featureName : streamFeatureGroup.getComplexFeatures()) {
      complexFeatureSchemas.put(featureName, streamFeatureGroup.getFeatureAvroSchema(featureName));
    }

    DataStream<GenericRecord> avroRecordDataStream =
            genericDataStream.map(new PojoToAvroRecord(
                            streamFeatureGroup.getAvroSchema(),
                            streamFeatureGroup.getEncodedAvroSchema(),
                            complexFeatureSchemas))
                    .returns(
                            new GenericRecordAvroTypeInfo(streamFeatureGroup.getDeserializedEncodedAvroSchema())
                    );

    return avroRecordDataStream.sinkTo(sink);
  }

  @Override
  public String addFile(String filePath) throws IOException, FeatureStoreException {
    if (Strings.isNullOrEmpty(filePath) || filePath.startsWith("file://")) {
      // local path no need to do anything
      return filePath;
    }

    String targetPath = FileUtils.getCurrentWorkingDirectory().toString()
        + filePath.substring(filePath.lastIndexOf("/"));

    if (HopsworksClient.getInstance().getHopsworksHttpClient() instanceof HopsworksExternalClient) {
      // Hopsworks external client, download the file using the rest API
      try (FileOutputStream outputStream = new FileOutputStream(targetPath)) {
        outputStream.write(DatasetApi.readContent(filePath));
      }

      return targetPath;
    } else {
      if (!filePath.startsWith("hdfs://")) {
        // make sure the path starts with hdfs:// to avoid issues in the cloning.
        filePath = "hdfs://" + filePath;
      }
      // Hopsworks internal client
      FileUtils.copy(new Path(filePath), new Path(targetPath), false);
      return targetPath;
    }
  }

  @Override
  public Map<String, String> getKafkaConfig(FeatureGroupBase featureGroup, Map<String, String> writeOptions)
          throws FeatureStoreException, IOException {
    boolean external = !(System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS)
            || (writeOptions != null
            && Boolean.parseBoolean(writeOptions.getOrDefault("internal_kafka", "false"))));

    StorageConnector.KafkaConnector storageConnector =
            storageConnectorApi.getKafkaStorageConnector(featureGroup.getFeatureStore(), external);
    storageConnector.setSslTruststoreLocation(addFile(storageConnector.getSslTruststoreLocation()));
    storageConnector.setSslKeystoreLocation(addFile(storageConnector.getSslKeystoreLocation()));

    Map<String, String> config = storageConnector.kafkaOptions();

    // To avoid distribution issues of the certificates across multiple pods/nodes
    // here we are extracting the key/certificates from the JKS keyStore/trustStore and
    // pass them in the configuration as PEM content
    try {
      KeyStore keyStore = KeyStore.getInstance("JKS");
      keyStore.load(new FileInputStream(storageConnector.getSslKeystoreLocation()),
              storageConnector.getSslKeystorePassword().toCharArray());
      config.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, getKey(keyStore, storageConnector.getSslKeystorePassword()));
      config.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, getCertificateChain(keyStore));
      config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");

      KeyStore trustStore = KeyStore.getInstance("JKS");
      trustStore.load(new FileInputStream(storageConnector.getSslTruststoreLocation()),
              storageConnector.getSslTruststorePassword().toCharArray());
      config.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, getRootCA(trustStore));
      config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
    } catch (Exception ex) {
      throw new IOException(ex);
    }

    // Remove the keystore and truststore location from the properties otherwise
    // the SSL engine will try to use them first.
    config.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    config.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    config.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    config.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
    config.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);

    if (writeOptions != null) {
      config.putAll(writeOptions);
    }
    config.put("enable.idempotence", "false");
    return config;
  }

  private String getKey(KeyStore keyStore, String password)
          throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
    String keyAlias = keyStore.aliases().nextElement();
    return "-----BEGIN PRIVATE KEY-----\n"
            + Base64.encodeBytes(keyStore.getKey(keyAlias, password.toCharArray()).getEncoded())
            + "\n-----END PRIVATE KEY-----";
  }

  private String getCertificateChain(KeyStore keyStore) throws KeyStoreException, CertificateEncodingException {
    String certificateAlias = keyStore.aliases().nextElement();
    Certificate[] certificateChain = keyStore.getCertificateChain(certificateAlias);

    StringBuilder certificateChainBuilder = new StringBuilder();
    for (Certificate certificate : certificateChain) {
      certificateChainBuilder.append("-----BEGIN CERTIFICATE-----\n")
              .append(Base64.encodeBytes(certificate.getEncoded()))
              .append("\n-----END CERTIFICATE-----\n");
    }

    return certificateChainBuilder.toString();
  }

  private String getRootCA(KeyStore trustStore) throws KeyStoreException, CertificateEncodingException {
    String rootCaAlias = trustStore.aliases().nextElement();
    return "-----BEGIN CERTIFICATE-----\n"
            + Base64.encodeBytes(trustStore.getCertificate(rootCaAlias).getEncoded())
            + "\n-----END CERTIFICATE-----";
  }

  @VisibleForTesting
  public void setStorageConnectorApi(StorageConnectorApi storageConnectorApi) {
    this.storageConnectorApi = storageConnectorApi;
  }
}
