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

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;

import lombok.Getter;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;

public class FlinkEngine {
  private static FlinkEngine INSTANCE = null;

  public static synchronized FlinkEngine getInstance() throws FeatureStoreException {
    if (INSTANCE == null) {
      INSTANCE = new FlinkEngine();
    }
    return INSTANCE;
  }

  @Getter
  private StreamExecutionEnvironment streamExecutionEnvironment;

  private final KafkaApi kafkaApi = new KafkaApi();
  private final HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();

  private final Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
  private final ConfigOption<String> keyStorePath =
      key("flink.hadoop.hops.ssl.keystore.name")
        .stringType()
        .defaultValue("trustStore.jks")
        .withDescription("path to keyStore.jks");
  private final ConfigOption<String> trustStorePath =
      key("flink.hadoop.hops.ssl.truststore.name")
        .stringType()
        .defaultValue("trustStore.jks")
        .withDescription("path to trustStore.jks");
  private final ConfigOption<String> materialPasswdPath =
      key("flink.hadoop.hops.ssl.keystores.passwd.name")
        .stringType()
        .defaultValue("material_passwd")
        .withDescription("path to material_passwd");

  private FlinkEngine() throws FeatureStoreException {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    // Configure the streamExecutionEnvironment
    streamExecutionEnvironment.getConfig().enableObjectReuse();
  }

  public DataStreamSink<?> writeDataStream(StreamFeatureGroup streamFeatureGroup, DataStream<?> dataStream,
      Map<String, String> writeOptions) throws FeatureStoreException, IOException {

    DataStream<Object> genericDataStream = (DataStream<Object>) dataStream;
    Properties properties = getKafkaProperties(streamFeatureGroup, writeOptions);

    KafkaSink<GenericRecord> sink = KafkaSink.<GenericRecord>builder()
        .setBootstrapServers(properties.getProperty("bootstrap.servers"))
        .setKafkaProducerConfig(properties)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
          .setTopic(streamFeatureGroup.getOnlineTopicName())
          .setKeySerializationSchema(new KeySerializationSchema(streamFeatureGroup.getPrimaryKeys()))
          .setValueSerializationSchema(new GenericRecordAvroSerializer())
          .build())
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
    Map<String, String> complexFeatureSchemas = new HashMap<>();
    for (String featureName: streamFeatureGroup.getComplexFeatures()) {
      complexFeatureSchemas.put(featureName, streamFeatureGroup.getFeatureAvroSchema(featureName));
    }

    DataStream<GenericRecord> avroRecordDataStream =
        genericDataStream.map(new PojoToAvroRecord(
          streamFeatureGroup.getDeserializedAvroSchema(),
          streamFeatureGroup.getDeserializedEncodedAvroSchema(),
        complexFeatureSchemas))
          .returns(
            new GenericRecordAvroTypeInfo(streamFeatureGroup.getDeserializedEncodedAvroSchema())
          );

    return avroRecordDataStream.sinkTo(sink);
  }

  private Properties getKafkaProperties(StreamFeatureGroup featureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {

    Properties properties = new Properties();
    boolean internalKafka = false;
    if (writeOptions != null) {
      internalKafka = Boolean.parseBoolean(writeOptions.getOrDefault("internal_kafka", "false"));
      properties.putAll(writeOptions);
    }

    if (System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS) || internalKafka) {
      properties.put("bootstrap.servers",
          kafkaApi.getBrokerEndpoints(featureGroup.getFeatureStore()).stream().map(broker -> broker.replaceAll(
          "INTERNAL://", ""))
            .collect(Collectors.joining(",")));
    } else {
      properties.put("bootstrap.servers",
          kafkaApi.getBrokerEndpoints(featureGroup.getFeatureStore(), true).stream()
            .map(broker -> broker.replaceAll("EXTERNAL://", ""))
            .collect(Collectors.joining(","))
      );
    }
    properties.put("security.protocol", "SSL");
    properties.put("ssl.truststore.location", client.getTrustStorePath());
    properties.put("ssl.truststore.password", client.getCertKey());
    properties.put("ssl.keystore.location", client.getKeyStorePath());
    properties.put("ssl.keystore.password", client.getCertKey());
    properties.put("ssl.key.password", client.getCertKey());
    properties.put("ssl.endpoint.identification.algorithm", "");
    return properties;
  }

  public String getTrustStorePath() {
    return flinkConfig.getString(trustStorePath);
  }

  public String getKeyStorePath() {
    return flinkConfig.getString(keyStorePath);
  }

  public String getCertKey() {
    return flinkConfig.getString(materialPasswdPath);
  }
}
