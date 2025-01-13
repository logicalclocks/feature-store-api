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

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.metadata.DatasetApi;
import com.logicalclocks.hsfs.engine.EngineBase;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import org.apache.avro.Schema;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BeamEngine extends EngineBase {
  private static BeamEngine INSTANCE = null;
  private FeatureGroupUtils featureGroupUtils = new FeatureGroupUtils();

  public static synchronized BeamEngine getInstance() throws FeatureStoreException {
    if (INSTANCE == null) {
      INSTANCE = new BeamEngine();
    }
    return INSTANCE;
  }

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
      getKafkaConfig(streamFeatureGroup, writeOptions),
      streamFeatureGroup.getDeserializedAvroSchema(), deserializedEncodedSchema, complexFeatureSchemas,
      streamFeatureGroup.getPrimaryKeys(), streamFeatureGroup);
  }

  @Override
  public String addFile(String filePath) throws IOException, FeatureStoreException {
    if (Strings.isNullOrEmpty(filePath)) {
      return filePath;
    }
    // this is used for unit testing
    if (!filePath.startsWith("file://")) {
      filePath = "hdfs://" + filePath;
    }
    String targetPath = System.getProperty("java.io.tmpdir") + filePath.substring(filePath.lastIndexOf("/"));
    try (FileOutputStream outputStream = new FileOutputStream(targetPath)) {
      outputStream.write(DatasetApi.readContent(filePath, featureGroupUtils.getDatasetType(filePath)));
    }
    return targetPath;
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

    if (writeOptions != null) {
      config.putAll(writeOptions);
    }
    return config;
  }
}
