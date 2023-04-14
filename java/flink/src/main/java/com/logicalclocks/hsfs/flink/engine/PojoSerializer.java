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

package com.logicalclocks.hsfs.flink.engine;

import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PojoSerializer implements SerializationSchema<Object> {

  Schema schema;

  public PojoSerializer(Schema schema) {
    this.schema = schema;
  }

  @SneakyThrows
  @Override
  public byte[] serialize(Object input) {
    return encode(input);
  }

  private byte[] encode(Object input) throws IOException {
    ReflectDatumWriter<Object> datumWriter = new ReflectDatumWriter<>(this.schema);
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
