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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PojoToGenericRecord<T> extends RichMapFunction<T, GenericRecord> implements
    ResultTypeQueryable<GenericRecord> {

  private final String schema;
  private final Map<String, String> complexFeatureSchemas;

  // Schema cannot be serialized thus it is created in open().
  private transient Schema deserializedSchema;
  private transient Map<String, Schema> deserializedComplexFeatureSchemas;

  public PojoToGenericRecord(String schema, Map<String, String> complexFeatureSchemas) {
    this.schema = schema;
    this.complexFeatureSchemas = complexFeatureSchemas;
  }

  @Override
  public GenericRecord map(T input) throws Exception {
    // Create a new Avro record based on the given schema
    GenericRecord record = new GenericData.Record(this.deserializedSchema);

    // Get the fields of the POJO class using reflection
    List<Field> fields =
        Arrays.stream(input.getClass().getDeclaredFields()).filter(f -> Modifier.isPublic(f.getModifiers()))
          .filter(f -> !f.getName().equals("SCHEMA$"))
        .collect(Collectors.toList());

    // Set the fields of the Avro record based on the values of the POJO fields
    if (fields.isEmpty()) {
      Field schemaField = input.getClass().getDeclaredField("SCHEMA$");
      schemaField.setAccessible(true);
      Schema fieldSchema = (Schema) schemaField.get(null);
      for (Schema.Field field : fieldSchema.getFields()) {
        String fieldName = field.name();
        Field pojoField = input.getClass().getDeclaredField(fieldName);
        pojoField.setAccessible(true);
        Object fieldValue = pojoField.get(input);
        populateAvroRecord(record, fieldName, fieldValue);
      }
    } else {
      for (Field field : fields) {
        String fieldName = field.getName();
        Object fieldValue = field.get(input);
        populateAvroRecord(record, fieldName, fieldValue);
      }
    }
    return record;
  }

  @Override
  public void open(Configuration configuration) throws Exception {
    super.open(configuration);
    this.deserializedSchema = new Schema.Parser().parse(this.schema);
    this.deserializedComplexFeatureSchemas = new HashMap<>();
    for (String featureName: this.complexFeatureSchemas.keySet()) {
      deserializedComplexFeatureSchemas.put(featureName,
            new Schema.Parser().parse(this.complexFeatureSchemas.get(featureName)));
    }
  }

  @Override
  public TypeInformation<GenericRecord> getProducedType() {
    return TypeInformation.of(GenericRecord.class);
  }

  private void populateAvroRecord(GenericRecord record, String fieldName, Object fieldValue) throws IOException {
    if (this.deserializedComplexFeatureSchemas.containsKey(fieldName)) {
      ReflectDatumWriter<Object> complexFeatureDatumWriter =
          new ReflectDatumWriter<>(this.deserializedComplexFeatureSchemas.get(fieldName));
      ByteArrayOutputStream complexFeatureByteArrayOutputStream = new ByteArrayOutputStream();
      complexFeatureByteArrayOutputStream.reset();
      BinaryEncoder complexFeatureBinaryEncoder =
          new EncoderFactory().binaryEncoder(complexFeatureByteArrayOutputStream, null);
      complexFeatureDatumWriter.write(fieldValue, complexFeatureBinaryEncoder);
      complexFeatureBinaryEncoder.flush();
      record.put(fieldName, complexFeatureByteArrayOutputStream.toByteArray());
    } else {
      record.put(fieldName, fieldValue);
    }
  }
}
