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

import com.logicalclocks.hsfs.engine.PojoToAvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

import java.util.Map;
import java.util.stream.Collectors;

public class PojoToAvroRecord<T> extends RichMapFunction<T, GenericRecord> implements
    ResultTypeQueryable<GenericRecord> {

  private final String featureGroupSchemaStr;
  private final String encodedFeatureGroupSchemaStr;
  private final Map<String, String> complexFeatureSchemasStr;

  // org.apache.avro.Schema$Field is not serializable. Create in open() and reused later on
  private transient Schema featureGroupSchema;
  private transient Schema encodedFeatureGroupSchema;
  private transient Map<String, Schema> complexFeatureSchemas;

  private transient GenericRecordAvroTypeInfo producedType;

  public PojoToAvroRecord(String featureGroupSchema,
                          String encodedFeatureGroupSchema,
                          Map<String, String> complexFeatureSchemas) {
    this.featureGroupSchemaStr = featureGroupSchema;
    this.encodedFeatureGroupSchemaStr = encodedFeatureGroupSchema;
    this.complexFeatureSchemasStr = complexFeatureSchemas;
  }

  @Override
  public GenericRecord map(T input) throws Exception {
    return PojoToAvroUtils.convertPojoToGenericRecord(
        input, featureGroupSchema, encodedFeatureGroupSchema, complexFeatureSchemas);
  }

  @Override
  public void open(Configuration configuration) throws Exception {
    this.featureGroupSchema = new Schema.Parser().parse(this.featureGroupSchemaStr);
    this.encodedFeatureGroupSchema = new Schema.Parser().parse(this.encodedFeatureGroupSchemaStr);
    this.complexFeatureSchemas = this.complexFeatureSchemasStr
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            e -> new Schema.Parser().parse(e.getValue())));
    this.producedType = new GenericRecordAvroTypeInfo(encodedFeatureGroupSchema);
  }

  @Override
  public TypeInformation<GenericRecord> getProducedType() {
    return producedType;
  }

}
