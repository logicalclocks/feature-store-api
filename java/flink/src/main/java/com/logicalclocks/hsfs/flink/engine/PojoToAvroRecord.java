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
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PojoToAvroRecord<T> extends RichMapFunction<T, GenericRecord> implements
    ResultTypeQueryable<GenericRecord> {

  private final String schema;
  private final String encodedSchema;
  private final Map<String, String> complexFeatureSchemas;

  // org.apache.avro.Schema$Field is not serializable. Create in open() and reused later on
  private transient Schema deserializedSchema;
  private transient Schema deserializedEncodedSchema;
  private transient Map<String, Schema> deserializedComplexFeatureSchemas;
  private transient GenericRecordAvroTypeInfo producedType;

  public PojoToAvroRecord(Schema schema, Schema encodedSchema,  Map<String, String> complexFeatureSchemas) {
    this.schema = schema.toString();
    this.encodedSchema = encodedSchema.toString();
    this.complexFeatureSchemas = complexFeatureSchemas;
  }

  @Override
  public GenericRecord map(T input) throws Exception {

    // validate
    validatePojoAgainstSchema(input, this.deserializedSchema);

    // Create a new Avro record based on the given schema
    GenericRecord record = new GenericData.Record(this.deserializedEncodedSchema);
    // Get the fields of the POJO class populate fields of the Avro record
    List<Field> fields =
        Arrays.stream(input.getClass().getDeclaredFields())
          .filter(f -> f.getName().equals("SCHEMA$"))
          .collect(Collectors.toList());
    if (!fields.isEmpty()) {
      // it means POJO was generated from avro schema
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
    this.deserializedEncodedSchema = new Schema.Parser().parse(this.encodedSchema);
    this.deserializedComplexFeatureSchemas = new HashMap<>();
    for (String featureName: this.complexFeatureSchemas.keySet()) {
      deserializedComplexFeatureSchemas.put(featureName,
            new Schema.Parser().parse(this.complexFeatureSchemas.get(featureName)));
    }
    this.producedType = new GenericRecordAvroTypeInfo(deserializedEncodedSchema);
  }

  @Override
  public TypeInformation<GenericRecord> getProducedType() {
    return producedType;
  }

  private void populateAvroRecord(GenericRecord record, String fieldName, Object fieldValue) throws IOException {
    if (this.deserializedComplexFeatureSchemas.containsKey(fieldName)) {
      GenericDatumWriter<Object> complexFeatureDatumWriter =
          new GenericDatumWriter<>(this.deserializedComplexFeatureSchemas.get(fieldName));
      ByteArrayOutputStream complexFeatureByteArrayOutputStream = new ByteArrayOutputStream();
      complexFeatureByteArrayOutputStream.reset();
      BinaryEncoder complexFeatureBinaryEncoder =
          new EncoderFactory().binaryEncoder(complexFeatureByteArrayOutputStream, null);
      complexFeatureDatumWriter.write(fieldValue, complexFeatureBinaryEncoder);
      complexFeatureBinaryEncoder.flush();
      record.put(fieldName, ByteBuffer.wrap(complexFeatureByteArrayOutputStream.toByteArray()));
      complexFeatureByteArrayOutputStream.flush();
      complexFeatureByteArrayOutputStream.close();
    } else {
      record.put(fieldName, fieldValue);
    }
  }

  private void validatePojoAgainstSchema(Object pojo, Schema avroSchema) throws SchemaValidationException {
    Schema pojoSchema = ReflectData.get().getSchema(pojo.getClass());
    SchemaValidatorBuilder builder = new SchemaValidatorBuilder();
    builder.canReadStrategy().validateAll().validate(avroSchema, Collections.singletonList(pojoSchema));
  }
}
