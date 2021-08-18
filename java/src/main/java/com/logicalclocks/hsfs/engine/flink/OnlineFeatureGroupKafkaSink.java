/*
 *  Copyright (c) 2022. Logical Clocks AB
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

package com.logicalclocks.hsfs.engine.flink;

import lombok.SneakyThrows;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class OnlineFeatureGroupKafkaSink implements KafkaSerializationSchema<byte[]> {
  private String keyField;
  private String topic;

  public OnlineFeatureGroupKafkaSink(String topic) {
    this.topic = topic;
  }

  public OnlineFeatureGroupKafkaSink(String keyField, String topic) {
    this.keyField = keyField;
    this.topic = topic;
  }

  @SneakyThrows
  @Override
  public ProducerRecord<byte[], byte[]> serialize(byte[] value, @Nullable Long timestamp) {
    byte[] key = this.keyField.getBytes();
    return new ProducerRecord(this.topic, key, value);
  }
}
