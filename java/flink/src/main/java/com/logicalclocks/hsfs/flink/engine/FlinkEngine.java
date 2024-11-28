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

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.engine.EngineBase;
import com.logicalclocks.hsfs.flink.StreamFeatureGroup;

import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import lombok.Getter;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FlinkEngine extends EngineBase {
  private static FlinkEngine INSTANCE = null;

  public static synchronized FlinkEngine getInstance() throws FeatureStoreException {
    if (INSTANCE == null) {
      INSTANCE = new FlinkEngine();
    }
    return INSTANCE;
  }

  @Getter
  private StreamExecutionEnvironment streamExecutionEnvironment;

  private FlinkEngine() throws FeatureStoreException {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    // Configure the streamExecutionEnvironment
    streamExecutionEnvironment.getConfig().enableObjectReuse();
  }

  public DataStreamSink<?> writeDataStream(StreamFeatureGroup streamFeatureGroup, DataStream<?> dataStream,
      Map<String, String> writeOptions) throws FeatureStoreException, IOException {

    DataStream<Object> genericDataStream = (DataStream<Object>) dataStream;
    Properties properties = new Properties();
    properties.putAll(getKafkaConfig(streamFeatureGroup, writeOptions));

    KafkaSink<GenericRecord> sink = KafkaSink.<GenericRecord>builder()
        .setBootstrapServers(properties.getProperty("bootstrap.servers"))
        .setKafkaProducerConfig(properties)
        .setRecordSerializer(new KafkaRecordSerializer(streamFeatureGroup))
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
    Map<String, String> complexFeatureSchemas = new HashMap<>();
    for (String featureName: streamFeatureGroup.getComplexFeatures()) {
      complexFeatureSchemas.put(featureName, streamFeatureGroup.getFeatureAvroSchema(featureName));
    }

    DataStream<GenericRecord> avroRecordDataStream =
        genericDataStream.map(new PojoToAvroRecord(
          streamFeatureGroup.getDeserializedAvroSchema(),
          streamFeatureGroup.getDeserializedEncodedAvroSchema(),
        complexFeatureSchemas))
          .returns(
            new GenericRecordAvroTypeInfo(streamFeatureGroup.getDeserializedEncodedAvroSchema())
          );

    return avroRecordDataStream.sinkTo(sink);
  }

  @Override
  public String addFile(String filePath) throws IOException {
    if (Strings.isNullOrEmpty(filePath)) {
      return filePath;
    }
    // this is used for unit testing
    if (!filePath.startsWith("file://")) {
      filePath = "hdfs://" + filePath;
    }
    String targetPath = FileUtils.getCurrentWorkingDirectory().toString()
        + filePath.substring(filePath.lastIndexOf("/"));
    FileUtils.copy(new Path(filePath), new Path(targetPath), false);
    return targetPath;
  }

  @Override
  public Map<String, String> getKafkaConfig(FeatureGroupBase featureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    boolean external = !(System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS)
        || (writeOptions != null
        && Boolean.parseBoolean(writeOptions.getOrDefault("internal_kafka", "false"))));

    StorageConnector.KafkaConnector storageConnector =
        storageConnectorApi.getKafkaStorageConnector(featureGroup.getFeatureStore(), external);
    storageConnector.setSslTruststoreLocation(addFile(storageConnector.getSslTruststoreLocation()));
    storageConnector.setSslKeystoreLocation(addFile(storageConnector.getSslKeystoreLocation()));

    Map<String, String> config = storageConnector.kafkaOptions();

    if (writeOptions != null) {
      config.putAll(writeOptions);
    }
    config.put("enable.idempotence", "false");
    return config;
  }
}
