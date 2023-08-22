package com.logicalclocks.hsfs.beam.engine;

import lombok.Setter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class BeamKafkaProducer extends KafkaProducer<String, GenericRecord> {
  @Setter
  public List<Header> headers = new ArrayList<>();

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
    for (Header header: headers) {
      record.headers().add(header);
    }
  }
}
