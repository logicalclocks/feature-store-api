/*
 * Copyright (c) 2020 Logical Clocks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import org.apache.hadoop.fs.FileSystem;

import lombok.SneakyThrows;

import scala.collection.Seq;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HudiEngine {

  private static final String HUDI_SPARK_FORMAT = "org.apache.hudi";
  private static final String HUDI_TABLE_NAME = "hoodie.table.name";
  private static final String HUDI_TABLE_STORAGE_TYPE = "hoodie.datasource.write.storage.type";
  private static final String HUDI_TABLE_OPERATION = "hoodie.datasource.write.operation";

  private static final String HUDI_KEY_GENERATOR_OPT_KEY = "hoodie.datasource.write.keygenerator.class";
  private static final String HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL = "org.apache.hudi.keygen.CustomKeyGenerator";
  private static final String HUDI_RECORD_KEY = "hoodie.datasource.write.recordkey.field";
  private static final String HUDI_PARTITION_FIELD = "hoodie.datasource.write.partitionpath.field";
  private static final String HUDI_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field";

  private static final String HUDI_HIVE_SYNC_ENABLE = "hoodie.datasource.hive_sync.enable";
  private static final String HUDI_HIVE_SYNC_TABLE = "hoodie.datasource.hive_sync.table";
  private static final String HUDI_HIVE_SYNC_DB = "hoodie.datasource.hive_sync.database";
  private static final String HUDI_HIVE_SYNC_JDBC_URL =
      "hoodie.datasource.hive_sync.jdbcurl";
  private static final String HUDI_HIVE_SYNC_PARTITION_FIELDS =
      "hoodie.datasource.hive_sync.partition_fields";
  private static final String HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY =
      "hoodie.datasource.hive_sync.partition_extractor_class";
  private static final String DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL =
      "org.apache.hudi.hive.MultiPartKeysValueExtractor";

  private static final String HUDI_COPY_ON_WRITE = "COPY_ON_WRITE";
  private static final String HUDI_QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type";
  private static final String HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental";
  private static final String HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime";
  private static final String HUDI_END_INSTANTTIME_OPT_KEY  = "hoodie.datasource.read.end.instanttime";

  private static final String PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class";
  private static final String PAYLOAD_CLASS_OPT_VAL =  "org.apache.hudi.common.model.EmptyHoodieRecordPayload";

  private Utils utils = new Utils();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
  private FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();


  public void saveHudiFeatureGroup(SparkSession sparkSession, FeatureGroup featureGroup,
                                   Dataset<Row> dataset, SaveMode saveMode, HudiOperationType operation,
                                   Map<String, String> writeOptions)
      throws IOException, FeatureStoreException {

    Map<String, String> hudiArgs = setupHudiWriteOpts(featureGroup, operation, writeOptions);

    dataset.write()
        .format(HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .mode(saveMode)
        .save(featureGroup.getLocation());

    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, featureGroup.getLocation());

    featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
  }

  public FeatureGroupCommit deleteRecord(SparkSession sparkSession, FeatureGroup featureGroup, Dataset<Row> deleteDF,
                                         Map<String, String> writeOptions) throws IOException, FeatureStoreException {

    Map<String, String> hudiArgs = setupHudiWriteOpts(featureGroup, HudiOperationType.UPSERT, writeOptions);
    hudiArgs.put(PAYLOAD_CLASS_OPT_KEY, PAYLOAD_CLASS_OPT_VAL);

    deleteDF.write().format(HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .mode(SaveMode.Append)
        .save(featureGroup.getLocation());

    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, featureGroup.getLocation());
    FeatureGroupCommit apiFgCommit = featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
    apiFgCommit.setCommitID(apiFgCommit.getCommitID());

    return apiFgCommit;
  }

  public void registerTemporaryTable(SparkSession sparkSession, FeatureGroup featureGroup, String alias,
                                     Long startTimestamp, Long  endTimestamp, Map<String, String> readOptions) {
    Map<String, String> hudiArgs = setupHudiReadOpts(startTimestamp, endTimestamp, readOptions);
    sparkSession.read()
        .format(HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .load(featureGroup.getLocation()).createOrReplaceTempView(alias);
  }

  private FeatureGroupCommit getLastCommitMetadata(SparkSession sparkSession, String basePath)
      throws IOException {
    FileSystem hopsfsConf = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
    HoodieTimeline commitTimeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(hopsfsConf, basePath);

    fgCommitMetadata.setCommitDateString(commitTimeline.lastInstant().get().getTimestamp());
    byte[] commitsToReturn = commitTimeline.getInstantDetails(commitTimeline.lastInstant().get()).get();
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(commitsToReturn,HoodieCommitMetadata.class);
    fgCommitMetadata.setRowsUpdated(commitMetadata.fetchTotalUpdateRecordsWritten());
    fgCommitMetadata.setRowsInserted(commitMetadata.fetchTotalInsertRecordsWritten());
    fgCommitMetadata.setRowsDeleted(commitMetadata.getTotalRecordsDeleted());
    return fgCommitMetadata;
  }

  private Map<String, String> setupHudiWriteOpts(FeatureGroup featureGroup, HudiOperationType operation,
                                                 Map<String, String> writeOptions)
      throws IOException, FeatureStoreException {
    Map<String, String> hudiArgs = new HashMap<String, String>();

    hudiArgs.put(HUDI_TABLE_STORAGE_TYPE, HUDI_COPY_ON_WRITE);

    hudiArgs.put(HUDI_KEY_GENERATOR_OPT_KEY, HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL);

    // primary keys
    Seq<String> primaryColumns = utils.getPrimaryColumns(featureGroup);
    if (primaryColumns.isEmpty()) {
      throw new FeatureStoreException("For time travel enabled feature groups You must provide at least 1 primary key");
    }
    hudiArgs.put(HUDI_RECORD_KEY, primaryColumns.mkString(","));

    // partition keys
    Seq<String> partitionColumns = utils.getPartitionColumns(featureGroup);
    if (!partitionColumns.isEmpty()) {
      hudiArgs.put(HUDI_PARTITION_FIELD, partitionColumns.mkString(":SIMPLE,") + ":SIMPLE");
      // For precombine key take 1st primary key
      hudiArgs.put(HUDI_PRECOMBINE_FIELD, primaryColumns.head());
      hudiArgs.put(HUDI_HIVE_SYNC_PARTITION_FIELDS, partitionColumns.mkString(","));
    }

    // table name
    String tableName = featureGroup.getName() + "_" + featureGroup.getVersion();
    hudiArgs.put(HUDI_TABLE_NAME, tableName);
    hudiArgs.put(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL);

    // Hive args
    hudiArgs.put(HUDI_HIVE_SYNC_ENABLE, "true");
    hudiArgs.put(HUDI_HIVE_SYNC_TABLE, tableName);
    String jdbcUrl = utils.getHiveMetastoreConnector(featureGroup);
    hudiArgs.put(HUDI_HIVE_SYNC_JDBC_URL, jdbcUrl);
    hudiArgs.put(HUDI_HIVE_SYNC_DB, featureGroup.getFeatureStore().getName());

    hudiArgs.put(HUDI_TABLE_OPERATION,operation.getValue());

    // Overwrite with user provided options if any
    if (writeOptions != null && !writeOptions.isEmpty()) {
      hudiArgs.putAll(writeOptions);
    }
    return hudiArgs;
  }

  private Map<String, String> setupHudiReadOpts(Long startTimestamp, Long endTimestamp,
                                                Map<String, String> readOptions) {
    Map<String, String> hudiArgs = new HashMap<String, String>();

    String hudiCommitStartTime = timeStampToHudiFormat(startTimestamp);
    String hudiCommitEndTime = timeStampToHudiFormat(endTimestamp);

    hudiArgs.put(HUDI_BEGIN_INSTANTTIME_OPT_KEY, hudiCommitStartTime);
    hudiArgs.put(HUDI_END_INSTANTTIME_OPT_KEY, hudiCommitEndTime);
    hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);

    // Overwrite with user provided options if any
    if (readOptions != null && !readOptions.isEmpty()) {
      hudiArgs.putAll(readOptions);
    }
    return hudiArgs;
  }

  @SneakyThrows
  public String timeStampToHudiFormat(Long commitedOnTimeStamp) {
    Date commitedOnDate = new Timestamp(commitedOnTimeStamp);
    return dateFormat.format(commitedOnDate);
  }
}
