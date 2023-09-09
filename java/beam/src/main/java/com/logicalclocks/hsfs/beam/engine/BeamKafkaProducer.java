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

import lombok.Setter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class BeamKafkaProducer extends KafkaProducer<String, GenericRecord> {
  @Setter
  private Map<String, byte[]> headerMap = new HashMap<>();

  public BeamKafkaProducer(Map configs) {
    super(configs);
  }

  public Future<RecordMetadata> send(ProducerRecord record) {
    addHeaders(record);
    return super.send(record);
  }

  public Future<RecordMetadata> send(ProducerRecord record, Callback callback) {
    addHeaders(record);
    return super.send(record, callback);
  }

  private void addHeaders(ProducerRecord record) {
    for (Map.Entry<String, byte[]> entry: headerMap.entrySet()) {
      record.headers().add(entry.getKey(), entry.getValue());
    }
  }
}
