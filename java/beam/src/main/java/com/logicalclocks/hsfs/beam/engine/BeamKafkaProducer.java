package com.logicalclocks.hsfs.beam.engine;

import lombok.Setter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class BeamKafkaProducer extends KafkaProducer<String, GenericRecord> {
  @Setter
  private Map<String, byte[]> headerMap = new HashMap<>();

  public BeamKafkaProducer(Map configs) {
    super(configs);
  }

  public Future<RecordMetadata> send(ProducerRecord record) {
    addHeaders(record);
    return super.send(record);
  }

  public Future<RecordMetadata> send(ProducerRecord record, Callback callback) {
    addHeaders(record);
    return super.send(record, callback);
  }

  private void addHeaders(ProducerRecord record) {
    for (Map.Entry<String, byte[]> entry: headerMap.entrySet()) {
      record.headers().add(entry.getKey(), entry.getValue());
    }
  }
}
