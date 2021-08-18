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

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;

import lombok.Getter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FlinkEngine {
  private static FlinkEngine INSTANCE = null;

  public static synchronized FlinkEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new FlinkEngine();
    }
    return INSTANCE;
  }

  @Getter
  private StreamExecutionEnvironment streamExecutionEnvironment;

  private FeatureGroupUtils utils = new FeatureGroupUtils();

  private FlinkEngine() {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    // Configure the streamExecutionEnvironment
    streamExecutionEnvironment.getConfig().enableObjectReuse();

    // TODO (Davit): allow users to add checkpointing interval
    // streamExecutionEnvironment.enableCheckpointing(30000);
  }

  // TODO (DAVIT): This function needs refactoring to be in par with Spark
  // Another option will be to somehow let spark save fg metadata (may be in deltastreamer?),
  // as DeltaStreamer is used anyway.
  public <S> List<Feature> parseFeatureGroupSchema(S datasetGeneric) throws FeatureStoreException {
    if (!(datasetGeneric instanceof ResolvedSchema)) {
      throw new FeatureStoreException("For this operation only org.apache.flink.table.catalog.ResolvedSchema class is "
          + "supported");
    }
    List<Feature> features = new ArrayList<>();
    ResolvedSchema tableSchema =  (ResolvedSchema) datasetGeneric;
    //  corresponds to spark's catalogString()
    for (Column column : tableSchema.getColumns()) {
      Feature f = new Feature(column.getName().toLowerCase(),
          column.getDataType().toString().toLowerCase(), false, false);
      features.add(f);
    }
    return features;
  }

  public <S> S writeDataStream(StreamFeatureGroup streamFeatureGroup, S dataStreamGeneric,
                               Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {

    DataStream<Map<String, Object>> dataStream = (DataStream<Map<String, Object>>) dataStreamGeneric;
    return (S) dataStream
        .map(new OnlineFeatureGroupGenericRecordWriter(streamFeatureGroup.getDeserializedAvroSchema()))
        .rescale()
        .rebalance()
        .addSink(new FlinkKafkaProducer<byte[]>(streamFeatureGroup.getOnlineTopicName(),
            new OnlineFeatureGroupKafkaSink(streamFeatureGroup.getPrimaryKeys().get(0),
                streamFeatureGroup.getOnlineTopicName()),
            utils.getKafkaProperties(streamFeatureGroup, writeOptions),
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));
  }

  public <S> S sanitizeFeatureNames(S datasetGeneric) throws FeatureStoreException {
    if (!(datasetGeneric instanceof ResolvedSchema)) {
      throw new FeatureStoreException("For this operation only org.apache.flink.table.catalog.ResolvedSchema class "
          + "is supported");
    }
    ResolvedSchema schema = (ResolvedSchema) datasetGeneric;
    List<Column> sanitizedFeatureNames =
        schema.getColumns().stream().map(f -> Column.physical(f.getName().toLowerCase(), f.getDataType())).collect(
            Collectors.toList());

    return (S) ResolvedSchema.of(sanitizedFeatureNames);
  }
}
