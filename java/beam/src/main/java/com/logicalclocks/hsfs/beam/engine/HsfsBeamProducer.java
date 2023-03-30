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

public class HsfsBeamProducer extends PTransform<@NonNull PCollection<Row>, @NonNull PDone> {
  String topic;
  Map<String, Object> properties;
  Schema schema;

  public HsfsBeamProducer(String topic, Map<String, Object> properties, Schema schema) {
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
