/*
 *  Copyright (c) 2021-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark.engine.hudi;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.AvroSource;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class DeltaStreamerKafkaSource extends AvroSource {
  private static final Logger LOG = LogManager.getLogger(DeltaStreamerKafkaSource.class);
  private static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  private static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";
  private final KafkaOffsetGen offsetGen;
  private final HoodieDeltaStreamerMetrics metrics;

  public DeltaStreamerKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                                  SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class);
    String deserializerClassName =
        props.getString(DataSourceWriteOptions.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS().key(), "");
    if (deserializerClassName.isEmpty()) {
      props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, DeltaStreamerAvroDeserializer.class);
    } else {
      try {
        if (schemaProvider == null) {
          throw new HoodieIOException("SchemaProvider has to be set to use custom Deserializer");
        }

        props.put(DataSourceWriteOptions.SCHEMA_PROVIDER_CLASS_PROP(), schemaProvider.getClass().getName());
        props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, Class.forName(deserializerClassName));
      } catch (ClassNotFoundException var9) {
        String error = "Could not load custom avro kafka deserializer: " + deserializerClassName;
        LOG.error(error);
        throw new HoodieException(error, var9);
      }
    }

    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder();
    this.metrics = new HoodieDeltaStreamerMetrics(builder.withProperties(props).build());
    this.offsetGen = new KafkaOffsetGen(props);
  }

  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
    OffsetRange[] offsetRanges = this.offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit, this.metrics);
    long totalNewMsgs = KafkaOffsetGen.CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic: " + this.offsetGen.getTopicName()
        + " from offsets: " + Arrays.stream(offsetRanges)
            .map(r -> String.format("%s:%d", r.partition(), r.fromOffset())).collect(Collectors.joining(","))
        + " until offsets: " + Arrays.stream(offsetRanges)
            .map(r -> String.format("%s:%d", r.partition(), r.untilOffset())).collect(Collectors.joining(",")));
    if (totalNewMsgs <= 0L) {
      return new InputBatch(Option.empty(), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
    } else {
      JavaRDD<GenericRecord> newDataRdd = this.toRdd(offsetRanges);
      return new InputBatch(Option.of(newDataRdd), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
    }
  }

  private JavaRDD<GenericRecord> toRdd(OffsetRange[] offsetRanges) {
    return KafkaUtils.createRDD(this.sparkContext, this.offsetGen.getKafkaParams(), offsetRanges,
        LocationStrategies.PreferConsistent())
            .filter(obj -> obj.value() != null)
            .map(obj -> (GenericRecord) obj.value());
  }

  @Override
  public void onCommit(String lastCkptStr) {
    if (this.props.getBoolean(KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.key(),
        KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.defaultValue())) {
      LOG.info("Committing offset: " + lastCkptStr);
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}
