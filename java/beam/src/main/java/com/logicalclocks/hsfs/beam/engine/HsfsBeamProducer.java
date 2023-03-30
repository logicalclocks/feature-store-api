package com.logicalclocks.hsfs.beam.engine;

import lombok.NonNull;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.VoidSerializer;
import java.util.Map;

public class HsfsBeamProducer extends PTransform<@NonNull PCollection<Object>, @NonNull PDone> {
  String topic;
  Map<String, Object> properties;

  public HsfsBeamProducer(String topic, Map<String, Object> properties) {
    this.topic = topic;
    this.properties = properties;
  }

  @Override
  public PDone expand(PCollection<Object> input) {
    return input.apply("Convert To KV", ParDo.of(new DoFn<Object, KV<Void, Object>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(KV.of(null, c.element()));
          }
        })
      )
      .apply(KafkaIO.<Void, Object>write()
        .withBootstrapServers(properties.get("bootstrap.servers").toString())
        .withTopic(topic)
        .withProducerConfigUpdates(properties)
        .withKeySerializer(VoidSerializer.class)
        .withValueSerializer(PojoSerializer.class)
        .withInputTimestamp()
      );
  }
}
