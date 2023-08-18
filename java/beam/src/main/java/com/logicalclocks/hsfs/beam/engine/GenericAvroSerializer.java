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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GenericAvroSerializer implements Serializer<GenericRecord> {

  @Override
  public byte[] serialize(String topic, GenericRecord genericRecord) {
    DatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<>(genericRecord.getSchema());
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.reset();

    List<GenericRecord> records = new ArrayList<>();
    records.add(genericRecord);

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
