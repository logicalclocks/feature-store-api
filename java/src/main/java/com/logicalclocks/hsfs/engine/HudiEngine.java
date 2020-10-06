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
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.util.Constants;

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import org.apache.hadoop.fs.FileSystem;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import lombok.Getter;
import lombok.SneakyThrows;

import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HudiEngine extends FeatureGroupBaseEngine  {

  @Getter
  private String basePath;
  @Getter
  private String tableName;

  private static final Logger LOGGER = LoggerFactory.getLogger(HudiEngine.class);
  private Utils utils = new Utils();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

  public FeatureGroupCommit saveHudiFeatureGroup(SparkSession sparkSession, FeatureGroup featureGroup,
                                                 Dataset<Row> dataset, SaveMode saveMode, String operation)
      throws IOException, FeatureStoreException {

    // TODO (davit): make sure writing completed without exception
    FeatureGroupCommit fgCommit = writeHudiDataset(sparkSession, featureGroup, dataset, saveMode, operation);

    FeatureGroupCommit apiFgCommit = featureGroupApi.featureGroupCommit(featureGroup, fgCommit);

    return apiFgCommit;
  }

  private FeatureGroupCommit writeHudiDataset(SparkSession sparkSession, FeatureGroup featureGroup,
                                              Dataset<Row> dataset, SaveMode saveMode, String operation)
      throws IOException, FeatureStoreException {

    Map<String, String> hudiArgs = setupHudiWriteOpts(featureGroup);

    List<String> supportedOps = Arrays.asList(Constants.HUDI_UPSERT, Constants.HUDI_INSERT,
        Constants.HUDI_BULK_INSERT);
    if (!supportedOps.stream().anyMatch(x -> x.equalsIgnoreCase(operation))) {
      throw new IllegalArgumentException("Unknown operation " + operation + " is provided. Please use either "
          + Constants.HUDI_INSERT + ", " + Constants.HUDI_INSERT + " or " + Constants.HUDI_BULK_INSERT);
    }

    hudiArgs.put(Constants.HUDI_TABLE_OPERATION,operation);

    DataFrameWriter<Row> writer = dataset.write().format(Constants.HUDI_SPARK_FORMAT);

    for (Map.Entry<String, String> entry : hudiArgs.entrySet()) {
      writer = writer.option(entry.getKey(), entry.getValue());
    }

    writer = writer.mode(saveMode);
    writer.save(this.basePath);

    FeatureGroupCommit featureGroupCommit = getLastCommitMetadata(sparkSession, this.basePath);
    return featureGroupCommit;
  }

  public FeatureGroupCommit deleteRecord(SparkSession sparkSession, FeatureGroup featureGroup, Dataset<Row> deleteDF)
      throws IOException, FeatureStoreException {

    Map<String, String> hudiArgs = setupHudiWriteOpts(featureGroup);
    //hudiArgs.put(Constants.PAYLOAD_CLASS_OPT_KEY, Constants.PAYLOAD_CLASS_OPT_VAL);
    //hudiArgs.put(Constants.HUDI_TABLE_OPERATION, Constants.HUDI_UPSERT);
    hudiArgs.put(Constants.HUDI_TABLE_OPERATION, Constants.HUDI_DELETE);
    DataFrameWriter<Row> writer =  deleteDF.write().format(Constants.HUDI_SPARK_FORMAT);

    for (Map.Entry<String, String> entry : hudiArgs.entrySet()) {
      writer = writer.option(entry.getKey(), entry.getValue());
    }

    writer = writer.mode(SaveMode.Append);
    writer.save(this.basePath);

    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, this.basePath);
    FeatureGroupCommit apiFgCommit = featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
    apiFgCommit.setCommitID(apiFgCommit.getCommitID());

    return apiFgCommit;
  }

  // TODO (davit):
  private Integer rollback(SparkSession sparkSession, String instantTime, String basePath) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    if (client.rollback(instantTime)) {
      LOGGER.info(String.format("The commit \"%s\" rolled back.", instantTime));
      return 0;
    } else {
      LOGGER.warn(String.format("The commit \"%s\" failed to roll back.", instantTime));
      return -1;
    }
  }

  private Map<String, String> setupHudiWriteOpts(FeatureGroup featureGroup) throws IOException, FeatureStoreException {

    this.basePath = featureGroup.getLocation();
    this.tableName = featureGroup.getName() + "_" + featureGroup.getVersion();

    Map<String, String> hudiArgs = new HashMap<String, String>();

    hudiArgs.put(Constants.HUDI_TABLE_STORAGE_TYPE, Constants.HUDI_COPY_ON_WRITE);

    Seq<String> primaryColumns = utils.getPrimaryColumns(featureGroup);

    // primary key is required
    if (primaryColumns.isEmpty()) {
      throw new FeatureStoreException("For time travel enabled feature groups You must provide at least 1 primary key");
    }

    hudiArgs.put(Constants.HUDI_RECORD_KEY, primaryColumns.mkString(","));

    // BUG: https://github.com/apache/hudi/pull/1984
    // hudiArgs.put(Constants.HUDI_KEY_GENERATOR_OPT_KEY, Constants.HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL);

    Seq<String> partitionColumns = utils.getPartitionColumns(featureGroup);

    if (!partitionColumns.isEmpty()) {
      hudiArgs.put(Constants.HUDI_PARTITION_FIELD, partitionColumns.mkString(","));
      // For precombine key take 1st particion key
      hudiArgs.put(Constants.HUDI_PRECOMBINE_FIELD, partitionColumns.head());
      hudiArgs.put(Constants.HUDI_HIVE_SYNC_PARTITION_FIELDS, partitionColumns.mkString(","));
    }

    hudiArgs.put(Constants.HUDI_TABLE_NAME, this.tableName);
    hudiArgs.put(Constants.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,
                Constants.DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL);

    // Hive
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_ENABLE, "true");
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_TABLE, this.tableName);

    // Gets the JDBC Connector for the project's Hive metastore
    StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
    StorageConnector storageConnector = storageConnectorApi.getByNameAndType(featureGroup.getFeatureStore(),
                featureGroup.getFeatureStore().getName(), StorageConnectorType.JDBC);
    String connStr = storageConnector.getConnectionString();
    String pw = FileUtils.readFileToString(new File("material_passwd"));
    String jdbcUrl = connStr + "sslTrustStore=t_certificate;trustStorePassword="
                + pw + ";sslKeyStore=k_certificate;keyStorePassword=" + pw;

    hudiArgs.put(Constants.HUDI_HIVE_SYNC_JDBC_URL, jdbcUrl);
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_DB, featureGroup.getFeatureStore().getName());

    return hudiArgs;
  }

  //hudi time time travel sql query
  public void registerTemporaryTable(SparkSession sparkSession, FeatureGroup featureGroup, String alias,
                                     Long startTimestamp, Long  endTimestamp) {

    sparkSession.conf().set("spark.sql.hive.convertMetastoreParquet", "false");
    sparkSession.sparkContext().hadoopConfiguration().setClass("mapreduce.input.pathFilter.class",
        org.apache.hudi.hadoop.HoodieROTablePathFilter.class, org.apache.hadoop.fs.PathFilter.class);

    Map<String, String> hudiArgs = setupHudiReadOpts(featureGroup, startTimestamp,
        endTimestamp);

    DataFrameReader queryDataset = sparkSession.read().format(Constants.HUDI_SPARK_FORMAT);
    for (Map.Entry<String, String> entry : hudiArgs.entrySet()) {
      queryDataset = queryDataset.option(entry.getKey(), entry.getValue());
    }

    queryDataset.load(getBasePath()).registerTempTable(alias);
  }

  private Map<String, String> setupHudiReadOpts(FeatureGroup featureGroup, Long startTimestamp, Long endTimestamp) {
    Map<String, String> hudiArgs = new HashMap<String, String>();
    Integer numberOfpartitionCols = utils.getPartitionColumns(featureGroup).length();
    this.basePath = featureGroup.getLocation();

    //Snapshot query
    if (startTimestamp == null && endTimestamp == null) {
      hudiArgs.put(Constants.HUDI_QUERY_TYPE_OPT_KEY, Constants.HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL);
      this.basePath =  this.basePath  + StringUtils.repeat("/*", numberOfpartitionCols + 1);
    } else if (endTimestamp != null) {
      hudiArgs.put(Constants.HUDI_QUERY_TYPE_OPT_KEY, Constants.HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);
      String hudiCommitEndTime = timeStampToHudiFormat(endTimestamp);
      //point in time  query
      if (startTimestamp == null) {
        hudiArgs.put(Constants.HUDI_BEGIN_INSTANTTIME_OPT_KEY, "000");
        hudiArgs.put(Constants.HUDI_END_INSTANTTIME_OPT_KEY, hudiCommitEndTime);
      } else if (startTimestamp != null) {       //incremental  query
        String hudiCommitStartTime = timeStampToHudiFormat(startTimestamp);
        hudiArgs.put(Constants.HUDI_BEGIN_INSTANTTIME_OPT_KEY, hudiCommitStartTime);
        hudiArgs.put(Constants.HUDI_END_INSTANTTIME_OPT_KEY, hudiCommitEndTime);
      }
    }

    return hudiArgs;
  }

  private FeatureGroupCommit getLastCommitMetadata(SparkSession sparkSession, String basePath)
      throws IOException {

    FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();

    FileSystem hopsfsConf = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
    HoodieTimeline commitTimeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(hopsfsConf, basePath);

    fgCommitMetadata.setCommitDateString(commitTimeline.lastInstant().get().getTimestamp());
    byte[] commitsToReturn = commitTimeline.getInstantDetails(commitTimeline.lastInstant().get()).get();
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(commitsToReturn,HoodieCommitMetadata.class);
    long totalUpdateRecordsWritten = commitMetadata.fetchTotalUpdateRecordsWritten();
    fgCommitMetadata.setRowsUpdated(totalUpdateRecordsWritten);
    long totalInsertRecordsWritten = commitMetadata.fetchTotalInsertRecordsWritten();
    fgCommitMetadata.setRowsInserted(totalInsertRecordsWritten);
    long totalRecordsDeleted = commitMetadata.getTotalRecordsDeleted();
    fgCommitMetadata.setRowsDeleted(totalRecordsDeleted);
    return fgCommitMetadata;
  }

  @SneakyThrows
  private String timeStampToHudiFormat(Long commitedOnTimeStamp) {
    Date commitedOnDate = new Timestamp(commitedOnTimeStamp);
    String strCommitedOnDate = dateFormat.format(commitedOnDate);
    return strCommitedOnDate;
  }
}
