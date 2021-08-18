/*
 * Copyright (c) 2021 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine.hudi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;
import java.util.Map;

public class DeltaStreamerAvroDeserializer implements Deserializer<GenericRecord> {

  private Schema schema;
  private BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);

  public DeltaStreamerAvroDeserializer() {
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String featureGroupSchema = (String) configs.get(HudiEngine.FEATURE_GROUP_SCHEMA);
    this.schema = new Schema.Parser().parse(featureGroupSchema);
  }

  @Override
  public GenericRecord deserialize(String topic, byte[] data) {
    try {
      GenericRecord result = null;
      if (data != null) {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(this.schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, binaryDecoder);
        result = new GenericData.Record(this.schema);
        result = datumReader.read(result, decoder);
      }
      return result;
    } catch (Exception ex) {
      throw new SerializationException(
          "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
    }
  }

  @Override
  public void close() {

  }
}
