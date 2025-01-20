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
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

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
    Schema deserializedEncodedSchema = new Schema.Parser().parse(streamFeatureGroup.getEncodedAvroSchema());

    Properties kafkaProps = new Properties();
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProps.putAll(getKafkaConfig(streamFeatureGroup, writeOptions));

    KafkaRecordSerializer kafkaRecordSerializer = new KafkaRecordSerializer(streamFeatureGroup);

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps)) {
      for (Object input : featureData) {
        // validate
        validatePojoAgainstSchema(input, new Schema.Parser().parse(streamFeatureGroup.getAvroSchema()));

        GenericRecord genericRecord = pojoToAvroRecord(input, deserializedEncodedSchema, complexFeatureSchemas);
        ProducerRecord<byte[], byte[]> record = kafkaRecordSerializer.serialize(genericRecord);

        producer.send(record);
      }
      producer.flush();
    }
    return featureData;
  }

  public GenericRecord pojoToAvroRecord(Object input, Schema deserializedEncodedSchema,
                                        Map<String, Schema> complexFeatureSchemas)
          throws NoSuchFieldException, IOException, IllegalAccessException {

    // Create a new Avro record based on the given schema
    GenericRecord record = new GenericData.Record(deserializedEncodedSchema);
    // Get the fields of the POJO class and populate fields of the Avro record
    List<Field> fields =
            Arrays.stream(input.getClass().getDeclaredFields())
                    .filter(f -> f.getName().equals("SCHEMA$"))
                    .collect(Collectors.toList());
    if (!fields.isEmpty()) {
      // it means POJO was generated from avro schema
      Field schemaField = input.getClass().getDeclaredField("SCHEMA$");
      schemaField.setAccessible(true);
      Schema fieldSchema = (Schema) schemaField.get(null);
      for (Schema.Field field : fieldSchema.getFields()) {
        String fieldName = field.name();
        Field pojoField = input.getClass().getDeclaredField(fieldName);
        pojoField.setAccessible(true);
        Object fieldValue = pojoField.get(input);
        populateAvroRecord(record, fieldName, fieldValue, complexFeatureSchemas);
      }
    } else {
      for (Field field : fields) {
        String fieldName = field.getName();
        Object fieldValue = field.get(input);
        populateAvroRecord(record, fieldName, fieldValue, complexFeatureSchemas);
      }
    }
    return record;
  }

  private void populateAvroRecord(GenericRecord record, String fieldName, Object fieldValue,
                                  Map<String, Schema> complexFeatureSchemas) throws IOException {
    if (complexFeatureSchemas.containsKey(fieldName)) {
      GenericDatumWriter<Object> complexFeatureDatumWriter =
              new GenericDatumWriter<>(complexFeatureSchemas.get(fieldName));
      ByteArrayOutputStream complexFeatureByteArrayOutputStream = new ByteArrayOutputStream();
      complexFeatureByteArrayOutputStream.reset();
      BinaryEncoder complexFeatureBinaryEncoder =
              new EncoderFactory().binaryEncoder(complexFeatureByteArrayOutputStream, null);
      complexFeatureDatumWriter.write(fieldValue, complexFeatureBinaryEncoder);
      complexFeatureBinaryEncoder.flush();
      record.put(fieldName, ByteBuffer.wrap(complexFeatureByteArrayOutputStream.toByteArray()));
      complexFeatureByteArrayOutputStream.flush();
      complexFeatureByteArrayOutputStream.close();
    } else {
      record.put(fieldName, fieldValue);
    }
  }

  private void validatePojoAgainstSchema(Object pojo, Schema avroSchema) throws SchemaValidationException {
    Schema pojoSchema = ReflectData.get().getSchema(pojo.getClass());
    SchemaValidatorBuilder builder = new SchemaValidatorBuilder();
    builder.canReadStrategy().validateAll().validate(avroSchema, Collections.singletonList(pojoSchema));
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
