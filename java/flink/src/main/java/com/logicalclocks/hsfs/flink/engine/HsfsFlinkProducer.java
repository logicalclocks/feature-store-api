package com.logicalclocks.hsfs.flink.engine;

import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HsfsFlinkProducer implements SerializationSchema<Object> {
  // TODO (davit): input fg schema to check
  //Schema schema;

  @SneakyThrows
  @Override
  public byte[] serialize(Object input) {
    return encode(input);
  }

  private byte[] encode(Object input) throws IOException {
    // TODO (davit): check schema
    Schema schema = ReflectData.get().getSchema(input.getClass());

    ReflectDatumWriter<Object> datumWriter = new ReflectDatumWriter<>(schema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.reset();

    List<Object> records = new ArrayList<>();
    records.add(input);

    BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
    for (Object segment: records) {
      datumWriter.write(segment, binaryEncoder);
    }
    binaryEncoder.flush();
    return byteArrayOutputStream.toByteArray();
  }
}
