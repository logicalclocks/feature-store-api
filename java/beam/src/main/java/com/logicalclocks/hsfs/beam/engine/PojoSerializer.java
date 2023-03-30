package com.logicalclocks.hsfs.beam.engine;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PojoSerializer implements Serializer<Object> {

  @Override
  public byte[] serialize(String topic, Object input) {

    // TODO (Davit): check fg schema
    Schema schema = ReflectData.get().getSchema(input.getClass());

    ReflectDatumWriter<Object> datumWriter = new ReflectDatumWriter<>(schema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.reset();

    List<Object> records = new ArrayList<>();
    records.add(input);

    BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
    for (Object segment: records) {
      try {
        datumWriter.write(segment, binaryEncoder);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      binaryEncoder.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return byteArrayOutputStream.toByteArray();
  }
}
