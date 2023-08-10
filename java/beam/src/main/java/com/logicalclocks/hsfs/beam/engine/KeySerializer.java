package com.logicalclocks.hsfs.beam.engine;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class KeySerializer implements Serializer<GenericRecord> {

  List<String> primaryKeys;

  public KeySerializer(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  @Override
  public byte[] serialize(String topic, GenericRecord record) {
    List<String> primaryKeyValues = new ArrayList<>();
    for (String primaryKey: this.primaryKeys) {
      primaryKeyValues.add(record.get(primaryKey).toString());
    }
    return String.join(";", primaryKeyValues).getBytes(StandardCharsets.UTF_8);
  }
}
