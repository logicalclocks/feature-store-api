/*
 *  Copyright (c) 2025. Hopsworks AB
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

package com.logicalclocks.hsfs.engine;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.metadata.DatasetApi;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Engine<T> extends EngineBase {

  private static Engine INSTANCE = null;

  private FeatureGroupUtils featureGroupUtils = new FeatureGroupUtils();

  public static synchronized Engine getInstance() throws FeatureStoreException {
    if (INSTANCE == null) {
      INSTANCE = new Engine();
    }
    return INSTANCE;
  }

  private Engine() throws FeatureStoreException {
  }

  public List<T> writeStream(
      StreamFeatureGroup streamFeatureGroup,
      List<T> featureData,
      Map<String, String> writeOptions
  ) throws FeatureStoreException, IOException, SchemaValidationException, NoSuchFieldException, IllegalAccessException {

    Map<String, Schema> complexFeatureSchemas = new HashMap<>();
    for (Object featureName: streamFeatureGroup.getComplexFeatures()) {
      complexFeatureSchemas.put(featureName.toString(),
                  new Schema.Parser().parse(streamFeatureGroup.getFeatureAvroSchema(featureName.toString())));
    }
    Schema featureGroupSchema = new Schema.Parser().parse(streamFeatureGroup.getAvroSchema());
    Schema encodedFeatureGroupSchema = new Schema.Parser().parse(streamFeatureGroup.getEncodedAvroSchema());

    Properties kafkaProps = new Properties();
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProps.putAll(getKafkaConfig(streamFeatureGroup, writeOptions));

    KafkaRecordSerializer kafkaRecordSerializer = new KafkaRecordSerializer(streamFeatureGroup);

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps)) {
      for (Object input : featureData) {
        GenericRecord genericRecord = PojoToAvroUtils.convertPojoToGenericRecord(
            input, featureGroupSchema, encodedFeatureGroupSchema, complexFeatureSchemas);
        ProducerRecord<byte[], byte[]> record = kafkaRecordSerializer.serialize(genericRecord);

        producer.send(record);
      }
      producer.flush();
    }
    return featureData;
  }

  @Override
  public String addFile(String filePath) throws IOException, FeatureStoreException {
    if (Strings.isNullOrEmpty(filePath)) {
      return filePath;
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
    config.put("enable.idempotence", "false");
    return config;
  }
}
