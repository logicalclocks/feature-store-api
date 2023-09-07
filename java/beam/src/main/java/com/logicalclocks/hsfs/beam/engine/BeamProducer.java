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
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import lombok.NonNull;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BeamProducer extends PTransform<@NonNull PCollection<Row>, @NonNull PDone> {
  private String topic;
  private Map<String, String> properties;
  private transient Schema schema;
  private transient Schema encodedSchema;
  private Map<String, Schema> deserializedComplexFeatureSchemas;
  private List<String> primaryKeys;
  private final Map<String, byte[]> headerMap = new HashMap<>();

  public BeamProducer(String topic, Map<String, String> properties, Schema schema, Schema encodedSchema,
                      Map<String, Schema> deserializedComplexFeatureSchemas, List<String> primaryKeys,
                      StreamFeatureGroup streamFeatureGroup) throws FeatureStoreException, IOException {
    this.schema = schema;
    this.encodedSchema = encodedSchema;
    this.topic = topic;
    this.properties = properties;
    this.deserializedComplexFeatureSchemas = deserializedComplexFeatureSchemas;
    this.primaryKeys = primaryKeys;

    headerMap.put("projectId",
        String.valueOf(streamFeatureGroup.getFeatureStore().getProjectId()).getBytes(StandardCharsets.UTF_8));
    headerMap.put("featureGroupId", String.valueOf(streamFeatureGroup.getId()).getBytes(StandardCharsets.UTF_8));
    headerMap.put("subjectId",
        String.valueOf(streamFeatureGroup.getSubject().getId()).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public PDone expand(PCollection<Row> input) {

    PCollection<GenericRecord> featureGroupAvroRecord = input
        .apply("Convert to avro generic record", ParDo.of(new DoFn<Row, GenericRecord>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            GenericRecord genericRecord = AvroUtils.toGenericRecord(c.element(), schema);
            c.output(genericRecord);
          }
        })).setCoder(AvroCoder.of(GenericRecord.class, schema));

    if (!deserializedComplexFeatureSchemas.keySet().isEmpty()) {
      featureGroupAvroRecord = featureGroupAvroRecord
        .apply("Serialize complex features", ParDo.of(new DoFn<GenericRecord, GenericRecord>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws IOException {
            GenericRecord encodedRecord = new GenericData.Record(encodedSchema);
            for (Schema.Field field: c.element().getSchema().getFields()) {
              if (deserializedComplexFeatureSchemas.containsKey(field.name())) {
                GenericDatumWriter<Object> complexFeatureDatumWriter =
                    new GenericDatumWriter<>(deserializedComplexFeatureSchemas.get(field.name()));
                ByteArrayOutputStream complexFeatureByteArrayOutputStream = new ByteArrayOutputStream();
                complexFeatureByteArrayOutputStream.reset();
                BinaryEncoder complexFeatureBinaryEncoder =
                    new EncoderFactory().binaryEncoder(complexFeatureByteArrayOutputStream, null);
                complexFeatureDatumWriter.write(field.name(), complexFeatureBinaryEncoder);
                complexFeatureBinaryEncoder.flush();
                encodedRecord.put(field.name(), ByteBuffer.wrap(complexFeatureByteArrayOutputStream.toByteArray()));
              }
            }
            c.output(encodedRecord);
          }
        }));
    }

    return featureGroupAvroRecord.apply("Convert To KV of primaryKey:GenericRecord",
        ParDo.of(new DoFn<GenericRecord, KV<String, GenericRecord>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            List<String> primaryKeyValues = new ArrayList<>();
            for (String primaryKey: primaryKeys) {
              primaryKeyValues.add(c.element().get(primaryKey).toString());
            }
            c.output(KV.of(String.join(";", primaryKeyValues), c.element()));
          }
        })
    )
      .apply("Sync to online feature group kafka topic", KafkaIO.<String, GenericRecord>write()
        .withBootstrapServers(properties.get("bootstrap.servers").toString())
        .withTopic(topic)
        //.withProducerConfigUpdates(properties)
        .withKeySerializer(StringSerializer.class)
        .withValueSerializer(GenericAvroSerializer.class)
        .withInputTimestamp()
        .withProducerFactoryFn(props -> {
          // copy jks files from resources to dataflow workers
          try {
            Path keyStorePath = Paths.get(properties.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
            InputStream keyStoreStream = Objects.requireNonNull(BeamProducer.class.getClassLoader()
                .getResourceAsStream(keyStorePath.getFileName().toString()));
            if (!Files.exists(keyStorePath)) {
              Files.copy(keyStoreStream, keyStorePath, StandardCopyOption.REPLACE_EXISTING);
            }
            Path trustStorePath = Paths.get(properties.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            InputStream trustStoreStream = Objects.requireNonNull(BeamProducer.class.getClassLoader()
                .getResourceAsStream(trustStorePath.getFileName().toString()));
            if (!Files.exists(trustStorePath)) {
              Files.copy(trustStoreStream, trustStorePath, StandardCopyOption.REPLACE_EXISTING);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
          props.putAll(properties);
          BeamKafkaProducer producer = new BeamKafkaProducer(props);
          producer.setHeaderMap(headerMap);
          return producer;
        })
      );
  }
}
