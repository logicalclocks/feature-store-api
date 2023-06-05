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

package com.logicalclocks.hsfs.beam.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class BeamEngine {
  private static BeamEngine INSTANCE = null;

  public static synchronized BeamEngine getInstance() throws FeatureStoreException {
    if (INSTANCE == null) {
      INSTANCE = new BeamEngine();
    }
    return INSTANCE;
  }

  private final KafkaApi kafkaApi = new KafkaApi();
  private final HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();

  private BeamEngine() throws FeatureStoreException {
  }

  public BeamProducer insertStream(StreamFeatureGroup streamFeatureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    Map<String, Schema> complexFeatureSchemas = new HashMap<>();
    for (String featureName: streamFeatureGroup.getComplexFeatures()) {
      complexFeatureSchemas.put(featureName,
            new Schema.Parser().parse(streamFeatureGroup.getFeatureAvroSchema(featureName)));
    }
    Schema deserializedEncodedSchema = new Schema.Parser().parse(streamFeatureGroup.getEncodedAvroSchema());

    return new BeamProducer(streamFeatureGroup.getOnlineTopicName(),
      getKafkaProperties(streamFeatureGroup, writeOptions),
      streamFeatureGroup.getDeserializedAvroSchema(), deserializedEncodedSchema, complexFeatureSchemas,
      streamFeatureGroup.getPrimaryKeys());
  }

  private Map<String, String> getKafkaProperties(StreamFeatureGroup featureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    Map<String, String> properties = new HashMap<>();
    boolean internalKafka = false;
    if (writeOptions != null) {
      internalKafka = Boolean.parseBoolean(writeOptions.getOrDefault("internal_kafka", "false"));
    }
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/"
        + Paths.get(client.getTrustStorePath()).getFileName());
    properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/tmp/"
        + Paths.get(client.getKeyStorePath()).getFileName());
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, client.getCertKey());
    properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, client.getCertKey());
    properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, client.getCertKey());
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

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

    return properties;
  }
}
