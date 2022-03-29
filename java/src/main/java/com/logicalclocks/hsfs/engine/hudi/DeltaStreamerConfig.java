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


import org.apache.commons.lang3.EnumUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

public class DeltaStreamerConfig implements Serializable {

  private HoodieDeltaStreamer.Config deltaStreamerConfig(Map<String, String> writeOptions) {

    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();

    // base path for the target hoodie table.
    cfg.targetBasePath = writeOptions.get(HudiEngine.HUDI_BASE_PATH);
    // name of the target table
    cfg.targetTableName = writeOptions.get(HudiEngine.HUDI_TABLE_NAME);

    // hudi table type
    cfg.tableType = writeOptions.get(HudiEngine.HUDI_TABLE_STORAGE_TYPE);

    // Takes one of these values : UPSERT (default), INSERT
    if (writeOptions.containsKey("operation")
        && EnumUtils.isValidEnum(WriteOperationType.class, writeOptions.get("operation"))) {
      cfg.operation = WriteOperationType.valueOf(writeOptions.get("operation"));
    } else {
      cfg.operation = WriteOperationType.UPSERT;
    }

    if (writeOptions.containsKey(HudiEngine.INITIAL_CHECKPOINT_STRING)) {
      // Resume Delta Streamer from this checkpoint
      cfg.checkpoint = writeOptions.get(HudiEngine.INITIAL_CHECKPOINT_STRING);
    }

    // Enable syncing to hive metastore
    cfg.enableHiveSync = true;

    // Subclass of org.apache.hudi.utilities.sources to read data
    cfg.sourceClassName = HudiEngine.KAFKA_SOURCE;

    // subclass of org.apache.hudi.utilities.schema.SchemaProvider to attach schemas to input & target table data,
    cfg.schemaProviderClassName = HudiEngine.SCHEMA_PROVIDER;

    if (writeOptions.get(HudiEngine.MIN_SYNC_INTERVAL_SECONDS) != null) {
      // the min sync interval of each sync in continuous mode
      cfg.minSyncIntervalSeconds = Integer.parseInt(writeOptions.get(HudiEngine.MIN_SYNC_INTERVAL_SECONDS));
      // Delta Streamer runs in continuous mode running source-fetch -> Transform -> Hudi Write in loop
      cfg.continuousMode = true;
    }

    cfg.sparkMaster = HudiEngine.SPARK_MASTER;

    // A subclass or a list of subclasses of org.apache.hudi.utilities.transform.Transformer. Allows transforming raw
    // source Dataset to a target Dataset (conforming to target schema) before writing. Default : Not set.
    // E:g - org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which allows a SQL query templated to be
    // passed as a transformation function). Pass a comma-separated list of subclass names to chain the transformations
    cfg.transformerClassNames = new ArrayList<String>() {{
        add(HudiEngine.DELTA_STREAMER_TRANSFORMER);
      }};

    // Field within source record to decide how to break ties between records with same key in input data.
    cfg.sourceOrderingField =  writeOptions.get(HudiEngine.DELTA_SOURCE_ORDERING_FIELD_OPT_KEY);

    cfg.configs = new ArrayList<String>() {{
        // User provided options
        writeOptions.entrySet().stream().filter(e -> !e.getKey().startsWith("kafka."))
            .forEach(e -> add(e.getKey() + "=" + e.getValue()));
        // Kafka props
        writeOptions.entrySet().stream().filter(e -> e.getKey().startsWith("kafka."))
            .forEach(e -> add(e.getKey().replace("kafka.", "") + "=" + e.getValue()));
      }};

    return cfg;
  }

  public TypedProperties streamToHoodieTable(Map<String, String> writeOptions, SparkSession spark) throws Exception {
    HoodieDeltaStreamer deltaSync = new HoodieDeltaStreamer(
        deltaStreamerConfig(writeOptions), JavaSparkContext.fromSparkContext(spark.sparkContext()));
    return deltaSync.sync();
  }
}
