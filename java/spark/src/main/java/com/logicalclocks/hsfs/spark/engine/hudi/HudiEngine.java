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

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.constructor.FeatureGroupAlias;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.KafkaApi;

import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.spark.FeatureGroup;
import com.logicalclocks.hsfs.spark.FeatureStore;
import com.logicalclocks.hsfs.spark.StreamFeatureGroup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.HoodieDataSourceHelpers;

import org.apache.hudi.common.util.Option;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.parquet.Strings;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import org.apache.hadoop.fs.FileSystem;
import org.json.JSONArray;
import scala.collection.Seq;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;

public class HudiEngine {

  public static final String HUDI_SPARK_FORMAT = "org.apache.hudi";

  protected static final String HUDI_BASE_PATH = "hoodie.base.path";
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
  protected static final String HUDI_HIVE_SYNC_MODE =
      "hoodie.datasource.hive_sync.mode";
  protected static final String HUDI_HIVE_SYNC_MODE_VAL = "hms";
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
  protected static final String HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot";
  protected static final String HUDI_QUERY_TIME_TRAVEL_AS_OF_INSTANT = "as.of.instant";
  protected static final String HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime";
  protected static final String HUDI_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime";

  protected static final String HUDI_WRITE_INSERT_DROP_DUPLICATES = "hoodie.datasource.write.insert.drop.duplicates";

  protected static final String PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class";
  protected static final String PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload";

  protected static final String HUDI_KAFKA_TOPIC = "hoodie.deltastreamer.source.kafka.topic";
  protected static final String COMMIT_METADATA_KEYPREFIX_OPT_KEY = "hoodie.datasource.write.commitmeta.key.prefix";
  protected static final String DELTASTREAMER_CHECKPOINT_KEY = "deltastreamer.checkpoint.key";
  protected static final String INITIAL_CHECKPOINT_STRING = "initialCheckPointString";
  protected static final String FEATURE_GROUP_SCHEMA = "com.logicalclocks.hsfs.spark.StreamFeatureGroup.avroSchema";
  protected static final String FEATURE_GROUP_ENCODED_SCHEMA =
      "com.logicalclocks.hsfs.spark.StreamFeatureGroup.encodedAvroSchema";
  protected static final String FEATURE_GROUP_COMPLEX_FEATURES =
      "com.logicalclocks.hsfs.spark.StreamFeatureGroup.complexFeatures";
  protected static final String KAFKA_SOURCE = "com.logicalclocks.hsfs.spark.engine.hudi.DeltaStreamerKafkaSource";
  protected static final String SCHEMA_PROVIDER =
      "com.logicalclocks.hsfs.spark.engine.hudi.DeltaStreamerSchemaProvider";
  protected static final String DELTA_STREAMER_TRANSFORMER =
      "com.logicalclocks.hsfs.spark.engine.hudi.DeltaStreamerTransformer";
  protected static final String DELTA_SOURCE_ORDERING_FIELD_OPT_KEY = "sourceOrderingField";

  protected static final String MIN_SYNC_INTERVAL_SECONDS = "minSyncIntervalSeconds";
  protected static final String SPARK_MASTER = "yarn";
  protected static final String PROJECT_ID = "projectId";
  protected static final String FEATURE_STORE_NAME = "featureStoreName";
  protected static final String SUBJECT_ID = "subjectId";
  protected static final String FEATURE_GROUP_ID = "featureGroupId";
  protected static final String FEATURE_GROUP_NAME = "featureGroupName";
  protected static final String FEATURE_GROUP_VERSION = "featureGroupVersion";
  protected static final String FUNCTION_TYPE = "functionType";
  protected static final String STREAMING_QUERY = "streamingQuery";

  private static final Map<String, String> HUDI_DEFAULT_PARALLELISM = new HashMap<String, String>() {
    {
      put("hoodie.bulkinsert.shuffle.parallelism", "5");
      put("hoodie.insert.shuffle.parallelism", "5");
      put("hoodie.upsert.shuffle.parallelism", "5");
    }
  };


  private FeatureGroupUtils utils = new FeatureGroupUtils();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();
  private DeltaStreamerConfig deltaStreamerConfig = new DeltaStreamerConfig();
  private KafkaApi kafkaApi = new KafkaApi();
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();

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
    if (fgCommit != null) {
      fgCommit.setValidationId(validationId);
      featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
    }
  }

  public FeatureGroupCommit deleteRecord(SparkSession sparkSession, FeatureGroupBase featureGroup,
                                             Dataset<Row> deleteDF, Map<String, String> writeOptions)
      throws IOException, FeatureStoreException,
      ParseException {

    Map<String, String> hudiArgs = setupHudiWriteOpts(featureGroup, HudiOperationType.UPSERT, writeOptions);
    hudiArgs.put(PAYLOAD_CLASS_OPT_KEY, PAYLOAD_CLASS_OPT_VAL);

    deleteDF.write().format(HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .mode(SaveMode.Append)
        .save(featureGroup.getLocation());

    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, featureGroup.getLocation());
    if (fgCommit != null) {
      FeatureGroupCommit apiFgCommit = featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
      apiFgCommit.setCommitID(apiFgCommit.getCommitID());
      return apiFgCommit;
    } else {
      throw new FeatureStoreException("No commit information was found for this feature group");
    }
  }

  public void registerTemporaryTable(SparkSession sparkSession, FeatureGroupAlias featureGroupAlias,
                                     Map<String, String> readOptions) {

  }

  private FeatureGroupCommit getLastCommitMetadata(SparkSession sparkSession, String basePath)
      throws IOException, FeatureStoreException, ParseException {
    FileSystem hopsfsConf = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
    HoodieTimeline commitTimeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(hopsfsConf, basePath);
    Option<HoodieInstant> lastInstant = commitTimeline.lastInstant();
    if (lastInstant.isPresent()) {
      fgCommitMetadata.setCommitDateString(lastInstant.get().getTimestamp());
      fgCommitMetadata.setCommitTime(
          FeatureGroupUtils.getTimeStampFromDateString(lastInstant.get().getTimestamp()));
      fgCommitMetadata.setLastActiveCommitTime(
          FeatureGroupUtils.getTimeStampFromDateString(commitTimeline.firstInstant().get().getTimestamp())
      );

      byte[] commitsToReturn = commitTimeline.getInstantDetails(lastInstant.get()).get();
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(commitsToReturn, HoodieCommitMetadata.class);
      fgCommitMetadata.setRowsUpdated(commitMetadata.fetchTotalUpdateRecordsWritten());
      fgCommitMetadata.setRowsInserted(commitMetadata.fetchTotalInsertRecordsWritten());
      fgCommitMetadata.setRowsDeleted(commitMetadata.getTotalRecordsDeleted());
      return fgCommitMetadata;
    } else {
      return null;
    }
  }

  private Map<String, String> setupHudiWriteOpts(FeatureGroupBase featureGroup, HudiOperationType operation,
                                                 Map<String, String> writeOptions)
      throws FeatureStoreException {
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

    List<Feature> features = featureGroup.getFeatures();
    String precombineKey = features.stream().filter(Feature::getHudiPrecombineKey).findFirst()
        .orElseThrow(() -> new FeatureStoreException("Can't find hudi precombine key")).getName();

    hudiArgs.put(HUDI_PRECOMBINE_FIELD, precombineKey);

    // Hive args
    hudiArgs.put(HUDI_HIVE_SYNC_ENABLE, "true");
    hudiArgs.put(HUDI_HIVE_SYNC_MODE, HUDI_HIVE_SYNC_MODE_VAL);
    hudiArgs.put(HUDI_HIVE_SYNC_TABLE, tableName);
    hudiArgs.put(HUDI_HIVE_SYNC_DB, featureGroup.getFeatureStore().getName());
    hudiArgs.put(HIVE_AUTO_CREATE_DATABASE_OPT_KEY, HIVE_AUTO_CREATE_DATABASE_OPT_VAL);
    hudiArgs.put(HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP, "true");
    if (operation != null) {
      hudiArgs.put(HUDI_TABLE_OPERATION, operation.getValue());
    }
    hudiArgs.putAll(HUDI_DEFAULT_PARALLELISM);

    // Overwrite with user provided options if any
    if (writeOptions != null && !writeOptions.isEmpty()) {
      hudiArgs.putAll(writeOptions);
    }

    return hudiArgs;
  }

  public Map<String, String> setupHudiReadOpts(Long startTimestamp, Long endTimestamp,
                                                Map<String, String> readOptions) {
    Map<String, String> hudiArgs = new HashMap<>();
    if (endTimestamp == null && (startTimestamp == null || startTimestamp == 0)) {
      // snapshot query latest state
      hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL);
    } else if (endTimestamp != null && (startTimestamp == null || startTimestamp == 0)) {
      // snapshot query with end time
      hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL);
      hudiArgs.put(HUDI_QUERY_TIME_TRAVEL_AS_OF_INSTANT, utils.timeStampToHudiFormat(endTimestamp));
    } else if (endTimestamp == null && startTimestamp != null) {
      // incremental query with start time until now
      hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);
      hudiArgs.put(HUDI_BEGIN_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(startTimestamp));
    } else {
      // incremental query with start and end time
      hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);
      hudiArgs.put(HUDI_BEGIN_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(startTimestamp));
      hudiArgs.put(HUDI_END_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(endTimestamp));
    }

    // Overwrite with user provided options if any
    if (readOptions != null && !readOptions.isEmpty()) {
      hudiArgs.putAll(readOptions);
    }
    return hudiArgs;
  }

  private void createEmptyTable(SparkSession sparkSession, StreamFeatureGroup streamFeatureGroup)
      throws IOException, FeatureStoreException {
    Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
    Properties properties = new Properties();
    properties.putAll(setupHudiWriteOpts(streamFeatureGroup, null, null));
    HoodieTableMetaClient.initTableAndGetMetaClient(configuration, streamFeatureGroup.getLocation(), properties);
  }

  public void reconcileHudiSchema(SparkSession sparkSession,
                                  FeatureGroupAlias featureGroupAlias, Map<String, String> hudiArgs)
          throws FeatureStoreException {
    String fgTableName = utils.getTableName(featureGroupAlias.getFeatureGroup());
    String[] hudiSchema = sparkSession.table(featureGroupAlias.getAlias()).columns();
    String[] hiveSchema = sparkSession.table(fgTableName).columns();
    if (!sparkSchemasMatch(hudiSchema, hiveSchema)) {
      Dataset dataframe = sparkSession.table(fgTableName).limit(0);

      FeatureStore featureStore = (FeatureStore) featureGroupAlias.getFeatureGroup().getFeatureStore();

      try {
        FeatureGroup fullFG = featureStore.getFeatureGroup(
                featureGroupAlias.getFeatureGroup().getName(),
                featureGroupAlias.getFeatureGroup().getVersion());
        saveHudiFeatureGroup(sparkSession, fullFG, dataframe,
                  HudiOperationType.UPSERT, new HashMap<>(), null);
      } catch (IOException | ParseException e) {
        throw new FeatureStoreException("Error while reconciling HUDI schema.", e);
      }

      sparkSession.read()
              .format(HudiEngine.HUDI_SPARK_FORMAT)
              .options(hudiArgs)
              .load(featureGroupAlias.getFeatureGroup().getLocation())
              .createOrReplaceTempView(featureGroupAlias.getAlias());
    }
  }

  public boolean sparkSchemasMatch(String[] schema1, String[] schema2) {
    if (schema1 == null || schema2 == null) {
      return false;
    }
    if (schema1.length != schema2.length) {
      return false;
    }

    Arrays.sort(schema1);
    Arrays.sort(schema2);

    for (int i = 0; i < schema1.length; i++) {
      if (!schema1[i].equals(schema2[i])) {
        return false;
      }
    }
    return true;
  }

  public void streamToHoodieTable(SparkSession sparkSession, StreamFeatureGroup streamFeatureGroup,
                                  Map<String, String> writeOptions) throws Exception {

    Map<String, String> hudiWriteOpts = setupHudiWriteOpts(streamFeatureGroup, HudiOperationType.UPSERT,
        writeOptions);
    hudiWriteOpts.put(PROJECT_ID, String.valueOf(streamFeatureGroup.getFeatureStore().getProjectId()));
    hudiWriteOpts.put(FEATURE_STORE_NAME, streamFeatureGroup.getFeatureStore().getName());
    hudiWriteOpts.put(SUBJECT_ID, String.valueOf(streamFeatureGroup.getSubject().getId()));
    hudiWriteOpts.put(FEATURE_GROUP_ID, String.valueOf(streamFeatureGroup.getId()));
    hudiWriteOpts.put(FEATURE_GROUP_NAME, streamFeatureGroup.getName());
    hudiWriteOpts.put(FEATURE_GROUP_VERSION, String.valueOf(streamFeatureGroup.getVersion()));
    hudiWriteOpts.put(HUDI_TABLE_NAME, utils.getFgName(streamFeatureGroup));
    hudiWriteOpts.put(HUDI_BASE_PATH, streamFeatureGroup.getLocation());
    hudiWriteOpts.put(HUDI_KAFKA_TOPIC, streamFeatureGroup.getOnlineTopicName());
    hudiWriteOpts.put(FEATURE_GROUP_SCHEMA, streamFeatureGroup.getAvroSchema());
    hudiWriteOpts.put(FEATURE_GROUP_ENCODED_SCHEMA, streamFeatureGroup.getEncodedAvroSchema());
    hudiWriteOpts.put(FEATURE_GROUP_COMPLEX_FEATURES,
        new JSONArray(streamFeatureGroup.getComplexFeatures()).toString());
    hudiWriteOpts.put(DELTA_SOURCE_ORDERING_FIELD_OPT_KEY,
        hudiWriteOpts.get(HUDI_PRECOMBINE_FIELD));

    // set consumer group id
    hudiWriteOpts.put(ConsumerConfig.GROUP_ID_CONFIG, String.valueOf(streamFeatureGroup.getId()));

    // check if table was initiated and if not initiate
    Path basePath = new Path(streamFeatureGroup.getLocation());
    FileSystem fs = basePath.getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
    if (!fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))) {
      createEmptyTable(sparkSession, streamFeatureGroup);
      // set "kafka.auto.offset.reset": "earliest"
      hudiWriteOpts.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    // it is possible that table was generated from empty topic
    if (getLastCommitMetadata(sparkSession, streamFeatureGroup.getLocation()) == null) {
      // set "kafka.auto.offset.reset": "earliest"
      hudiWriteOpts.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    deltaStreamerConfig.streamToHoodieTable(hudiWriteOpts, sparkSession);
    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, streamFeatureGroup.getLocation());
    if (fgCommit != null) {
      featureGroupApi.featureGroupCommit(streamFeatureGroup, fgCommit);
      streamFeatureGroup.computeStatistics();
    }
  }
}
