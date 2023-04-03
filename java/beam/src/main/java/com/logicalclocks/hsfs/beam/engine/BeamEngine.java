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
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class BeamEngine {
  private static BeamEngine INSTANCE = null;

  public static synchronized BeamEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new BeamEngine();
    }
    return INSTANCE;
  }

  private KafkaApi kafkaApi = new KafkaApi();

  private BeamEngine() {
  }

  public BeamProducer insertStream(StreamFeatureGroup featureGroup) throws FeatureStoreException, IOException {
    return new BeamProducer(featureGroup.getOnlineTopicName(), getKafkaProperties(featureGroup),
      featureGroup.getDeserializedAvroSchema());
  }

  private Map<String, Object> getKafkaProperties(StreamFeatureGroup featureGroup) throws FeatureStoreException,
      IOException {
    Map<String, Object> properties = new HashMap<>();
    HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();

    properties.put("bootstrap.servers",
        kafkaApi.getBrokerEndpoints(featureGroup.getFeatureStore(), true).stream()
        .map(broker -> broker.replaceAll("EXTERNAL://", ""))
        .collect(Collectors.joining(","))
    );
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,  client.getTrustStorePath());
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, client.getCertKey());
    properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, client.getKeyStorePath());
    properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, client.getCertKey());
    properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, client.getCertKey());
    properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    return properties;
  }

}
