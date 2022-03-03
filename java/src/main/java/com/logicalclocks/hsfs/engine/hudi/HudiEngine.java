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

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.HoodieDataSourceHelpers;

import org.apache.parquet.Strings;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import org.apache.hadoop.fs.FileSystem;
import scala.collection.Seq;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HudiEngine {

  protected static final String HUDI_BASE_PATH = "hoodie.base.path";
  protected static final String HUDI_SPARK_FORMAT = "org.apache.hudi";
  protected static final String HUDI_TABLE_NAME = "hoodie.table.name";
  protected static final String HUDI_TABLE_STORAGE_TYPE = "hoodie.datasource.write.storage.type";
  protected static final String HUDI_TABLE_OPERATION = "hoodie.datasource.write.operation";

  protected static final String HUDI_KEY_GENERATOR_OPT_KEY = "hoodie.datasource.write.keygenerator.class";
  protected static final String HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL = "org.apache.hudi.keygen.CustomKeyGenerator";
  protected static final String HUDI_RECORD_KEY = "hoodie.datasource.write.recordkey.field";
  protected static final String HUDI_PARTITION_FIELD = "hoodie.datasource.write.partitionpath.field";
  protected static final String HUDI_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field";

  protected static final String HUDI_HIVE_SYNC_ENABLE = "hoodie.datasource.hive_sync.enable";
  protected static final String HUDI_HIVE_SYNC_TABLE = "hoodie.datasource.hive_sync.table";
  protected static final String HUDI_HIVE_SYNC_DB = "hoodie.datasource.hive_sync.database";
  protected static final String HUDI_HIVE_SYNC_JDBC_URL =
      "hoodie.datasource.hive_sync.jdbcurl";
  protected static final String HUDI_HIVE_SYNC_PARTITION_FIELDS =
      "hoodie.datasource.hive_sync.partition_fields";
  protected static final String HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY =
      "hoodie.datasource.hive_sync.partition_extractor_class";
  private static final String HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP =
      "hoodie.datasource.hive_sync.support_timestamp";
  protected static final String DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL =
      "org.apache.hudi.hive.MultiPartKeysValueExtractor";
  protected static final String HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL =
      "org.apache.hudi.hive.NonPartitionedExtractor";
  protected static final String HIVE_AUTO_CREATE_DATABASE_OPT_KEY = "hoodie.datasource.hive_sync.auto_create_database";
  protected static final String HIVE_AUTO_CREATE_DATABASE_OPT_VAL = "false";

  protected static final String HUDI_COPY_ON_WRITE = "COPY_ON_WRITE";
  protected static final String HUDI_QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type";
  protected static final String HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental";
  protected static final String HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime";
  protected static final String HUDI_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime";

  protected static final String HUDI_WRITE_INSERT_DROP_DUPLICATES = "hoodie.datasource.write.insert.drop.duplicates";

  protected static final String PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class";
  protected static final String PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload";

  protected static final String HUDI_KAFKA_TOPIC = "hoodie.deltastreamer.source.kafka.topic";
  protected static final String COMMIT_METADATA_KEYPREFIX_OPT_KEY = "hoodie.datasource.write.commitmeta.key.prefix";
  protected static final String DELTASTREAMER_CHECKPOINT_KEY = "deltastreamer.checkpoint.key";
  protected static final String CHECKPOINT_PROVIDER_PATH_PROP = "hoodie.deltastreamer.checkpoint.provider.path";
  protected static final String INITIAL_CHECKPOINT_PROVIDER =
      "com.logicalclocks.hsfs.engine.hudi.InitialCheckpointFromAnotherHoodieTimelineProvider";
  protected static final String FEATURE_GROUP_SCHEMA = "com.logicalclocks.hsfs.FeatureGroup.schema";
  protected static final String KAFKA_SOURCE = "com.logicalclocks.hsfs.engine.hudi.DeltaStreamerKafkaSource";
  protected static final String SCHEMA_PROVIDER = "com.logicalclocks.hsfs.engine.hudi.DeltaStreamerSchemaProvider";
  protected static final String DELTA_STREAMER_TRANSFORMER =
      "com.logicalclocks.hsfs.engine.hudi.DeltaStreamerTransformer";
  protected static final String DELTA_SOURCE_ORDERING_FIELD_OPT_KEY = "sourceOrderingField";

  protected static final String MIN_SYNC_INTERVAL_SECONDS = "minSyncIntervalSeconds";
  protected static final String SPARK_MASTER = "yarn";
  protected static final String PROJECT_ID = "projectId";
  protected static final String FEATURE_STORE_NAME = "featureStoreName";
  protected static final String FEATURE_GROUP_NAME = "featureGroupName";
  protected static final String FEATURE_GROUP_VERSION = "featureGroupVersion";
  protected static final String VALIDATION_ID = "validationId";
  protected static final String FUNCTION_TYPE = "functionType";
  protected static final String STREAMING_QUERY = "streamingQuery";

  private FeatureGroupUtils utils = new FeatureGroupUtils();

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();
  private DeltaStreamerConfig deltaStreamerConfig = new DeltaStreamerConfig();

  public void saveHudiFeatureGroup(SparkSession sparkSession, FeatureGroupBase featureGroup,
                                   Dataset<Row> dataset, HudiOperationType operation,
                                   Map<String, String> writeOptions, Integer validationId)
      throws IOException, FeatureStoreException, ParseException {

    Map<String, String> hudiArgs = setupHudiWriteOpts(featureGroup, operation, writeOptions);

    dataset.write()
        .format(HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .mode(SaveMode.Append)
        .save(featureGroup.getLocation());

    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, featureGroup.getLocation());
    fgCommit.setValidationId(validationId);

    featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
  }

  public <S> FeatureGroupCommit deleteRecord(SparkSession sparkSession, FeatureGroupBase featureGroup,
                                             S genericDeleteDF, Map<String, String> writeOptions)
      throws IOException, FeatureStoreException,
      ParseException {

    Dataset<Row> deleteDF = (Dataset<Row>) genericDeleteDF;
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
                                     Long startTimestamp, Long endTimestamp, Map<String, String> readOptions) {
    Map<String, String> hudiArgs = setupHudiReadOpts(startTimestamp, endTimestamp, readOptions);
    sparkSession.read()
        .format(HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .load(featureGroup.getLocation()).createOrReplaceTempView(alias);
  }

  private FeatureGroupCommit getLastCommitMetadata(SparkSession sparkSession, String basePath)
      throws IOException, FeatureStoreException, ParseException {
    FileSystem hopsfsConf = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
    HoodieTimeline commitTimeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(hopsfsConf, basePath);

    fgCommitMetadata.setCommitDateString(commitTimeline.lastInstant().get().getTimestamp());
    fgCommitMetadata.setCommitTime(utils.getTimeStampFromDateString(commitTimeline.lastInstant().get().getTimestamp()));
    byte[] commitsToReturn = commitTimeline.getInstantDetails(commitTimeline.lastInstant().get()).get();
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(commitsToReturn, HoodieCommitMetadata.class);
    fgCommitMetadata.setRowsUpdated(commitMetadata.fetchTotalUpdateRecordsWritten());
    fgCommitMetadata.setRowsInserted(commitMetadata.fetchTotalInsertRecordsWritten());
    fgCommitMetadata.setRowsDeleted(commitMetadata.getTotalRecordsDeleted());
    return fgCommitMetadata;
  }

  private Map<String, String> setupHudiWriteOpts(FeatureGroupBase featureGroup, HudiOperationType operation,
                                                Map<String, String> writeOptions)
      throws IOException, FeatureStoreException {
    Map<String, String> hudiArgs = new HashMap<String, String>();

    hudiArgs.put(HUDI_TABLE_STORAGE_TYPE, HUDI_COPY_ON_WRITE);

    hudiArgs.put(HUDI_KEY_GENERATOR_OPT_KEY, HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL);

    // primary keys
    String primaryColumns = utils.getPrimaryColumns(featureGroup).mkString(",");
    if (!Strings.isNullOrEmpty(featureGroup.getEventTime())) {
      primaryColumns = primaryColumns + "," + featureGroup.getEventTime();
    }
    hudiArgs.put(HUDI_RECORD_KEY, primaryColumns);

    // table name
    String tableName = utils.getFgName(featureGroup);
    hudiArgs.put(HUDI_TABLE_NAME, tableName);

    // partition keys
    Seq<String> partitionColumns = utils.getPartitionColumns(featureGroup);
    if (!partitionColumns.isEmpty()) {
      hudiArgs.put(HUDI_PARTITION_FIELD, partitionColumns.mkString(":SIMPLE,") + ":SIMPLE");
      hudiArgs.put(HUDI_HIVE_SYNC_PARTITION_FIELDS, partitionColumns.mkString(","));
      hudiArgs.put(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL);
    } else {
      hudiArgs.put(HUDI_PARTITION_FIELD, "");
      hudiArgs.put(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL);
    }

    String precombineKey = featureGroup.getFeatures().stream().filter(Feature::getHudiPrecombineKey).findFirst()
        .orElseThrow(() -> new FeatureStoreException("Can't find hudi precombine key")).getName();
    hudiArgs.put(HUDI_PRECOMBINE_FIELD, precombineKey);

    // Hive args
    hudiArgs.put(HUDI_HIVE_SYNC_ENABLE, "true");
    hudiArgs.put(HUDI_HIVE_SYNC_TABLE, tableName);
    hudiArgs.put(HUDI_HIVE_SYNC_JDBC_URL, utils.getHiveServerConnection(featureGroup));
    hudiArgs.put(HUDI_HIVE_SYNC_DB, featureGroup.getFeatureStore().getName());
    hudiArgs.put(HIVE_AUTO_CREATE_DATABASE_OPT_KEY, HIVE_AUTO_CREATE_DATABASE_OPT_VAL);
    hudiArgs.put(HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP, "true");

    hudiArgs.put(HUDI_TABLE_OPERATION, operation.getValue());

    // Overwrite with user provided options if any
    if (writeOptions != null && !writeOptions.isEmpty()) {
      hudiArgs.putAll(writeOptions);
    }
    return hudiArgs;
  }

  private Map<String, String> setupHudiReadOpts(Long startTimestamp, Long endTimestamp,
                                                Map<String, String> readOptions) {
    Map<String, String> hudiArgs = new HashMap<String, String>();

    if (startTimestamp != null) {
      hudiArgs.put(HUDI_BEGIN_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(startTimestamp));
    } else {
      hudiArgs.put(HUDI_BEGIN_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(0L));
    }

    hudiArgs.put(HUDI_END_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(endTimestamp));
    hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);

    // Overwrite with user provided options if any
    if (readOptions != null && !readOptions.isEmpty()) {
      hudiArgs.putAll(readOptions);
    }
    return hudiArgs;
  }

  private void createEmptyTable(StreamFeatureGroup streamFeatureGroup) throws IOException, FeatureStoreException {
    Configuration configuration = SparkEngine.getInstance().getSparkSession().sparkContext().hadoopConfiguration();
    Properties properties = new Properties();
    properties.putAll(setupHudiWriteOpts((FeatureGroupBase) streamFeatureGroup,
        HudiOperationType.BULK_INSERT, null));
    HoodieTableMetaClient.initTableAndGetMetaClient(configuration, streamFeatureGroup.getLocation(), properties);
  }

  public void streamToHoodieTable(SparkSession sparkSession, StreamFeatureGroup streamFeatureGroup,
                                  Map<String, String> writeOptions) throws Exception {

    Map<String, String> hudiWriteOpts = setupHudiWriteOpts(streamFeatureGroup, HudiOperationType.UPSERT,
        writeOptions);
    hudiWriteOpts.put(PROJECT_ID, String.valueOf(streamFeatureGroup.getFeatureStore().getProjectId()));
    hudiWriteOpts.put(FEATURE_STORE_NAME, streamFeatureGroup.getFeatureStore().getName());
    hudiWriteOpts.put(FEATURE_GROUP_NAME, streamFeatureGroup.getName());
    hudiWriteOpts.put(FEATURE_GROUP_VERSION, String.valueOf(streamFeatureGroup.getVersion()));
    hudiWriteOpts.put(HUDI_TABLE_NAME, utils.getFgName(streamFeatureGroup));
    hudiWriteOpts.put(HUDI_BASE_PATH, streamFeatureGroup.getLocation());
    hudiWriteOpts.put(CHECKPOINT_PROVIDER_PATH_PROP, streamFeatureGroup.getLocation());
    hudiWriteOpts.put(HUDI_KAFKA_TOPIC, streamFeatureGroup.getOnlineTopicName());
    hudiWriteOpts.put(FEATURE_GROUP_SCHEMA, streamFeatureGroup.getAvroSchema());
    hudiWriteOpts.put(DELTA_SOURCE_ORDERING_FIELD_OPT_KEY,
        hudiWriteOpts.get(HUDI_PRECOMBINE_FIELD));
    writeOptions.putAll(hudiWriteOpts);

    // check if table was initiated and if not initiate
    Path basePath = new Path(streamFeatureGroup.getLocation());
    FileSystem fs = basePath.getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
    if (fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))) {
      createEmptyTable(streamFeatureGroup);
    }

    TypedProperties typedProperties = deltaStreamerConfig.streamToHoodieTable(writeOptions, sparkSession);
    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, streamFeatureGroup.getLocation());
    fgCommit.setValidationId((Integer) typedProperties.get(VALIDATION_ID));
    featureGroupApi.featureGroupCommit(streamFeatureGroup, fgCommit);

    if (writeOptions.containsKey(FUNCTION_TYPE) && writeOptions.get(FUNCTION_TYPE)
        .equals(STREAMING_QUERY)) {
      streamFeatureGroup.computeStatistics();
    }
  }
}
