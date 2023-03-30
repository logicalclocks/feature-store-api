package com.logicalclocks.hsfs.beam.engine;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PojoSerializer implements Serializer<Row> {

  @Override
  public byte[] serialize(String topic, Row input) {

    DatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<>(AvroUtils.toAvroSchema(input.getSchema()));
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.reset();

    List<GenericRecord> records = new ArrayList<>();
    records.add(AvroUtils.toGenericRecord(input, AvroUtils.toAvroSchema(input.getSchema())));

    BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
    for (GenericRecord segment: records) {
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
