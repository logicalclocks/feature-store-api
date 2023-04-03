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

import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.VoidSerializer;
import java.util.Map;

public class BeamProducer extends PTransform<@NonNull PCollection<Row>, @NonNull PDone> {
  String topic;
  Map<String, Object> properties;
  Schema schema;

  public BeamProducer(String topic, Map<String, Object> properties, Schema schema) {
    this.schema = schema;
    this.topic = topic;
    this.properties = properties;
  }

  @Override
  public PDone expand(PCollection<Row> input) {
    return input
      .apply(ParDo.of(new DoFn<Row, GenericRecord>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          GenericRecord genericRecord = AvroUtils.toGenericRecord(c.element(), schema);
          c.output(genericRecord);
        }
      })).setCoder(AvroCoder.of(GenericRecord.class, schema))

      .apply(
        KafkaIO.<Void, GenericRecord>write()
        .withBootstrapServers(properties.get("bootstrap.servers").toString())
        .withTopic(topic)
        .withProducerConfigUpdates(properties)
        .withKeySerializer(VoidSerializer.class)
        .withValueSerializer(GenericAvroSerializer.class)
        .withInputTimestamp()
          .values()
      );
  }


}
