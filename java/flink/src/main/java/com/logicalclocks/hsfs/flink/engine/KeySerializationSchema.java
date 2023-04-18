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

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class KeySerializationSchema implements SerializationSchema<GenericRecord> {

  List<String> primaryKeys;

  public KeySerializationSchema(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  @Override
  public byte[] serialize(GenericRecord record) {
    List<String> primaryKeyValues = new ArrayList<>();
    for (String primaryKey: this.primaryKeys) {
      primaryKeyValues.add(record.get(primaryKey).toString());
    }
    return String.join(";", primaryKeyValues).getBytes(StandardCharsets.UTF_8);
  }
}
