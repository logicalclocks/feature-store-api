package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.util.Constants;

import lombok.Getter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HudiEngine {

  @Getter
  private String basePath;
  @Getter
  private String tableName;


  private Utils utils = new Utils();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();

  private Map<String, String> setupHudiWriteArgs(FeatureGroup featureGroup) throws IOException, FeatureStoreException {

    this.basePath = utils.getHudiBasePath(featureGroup);
    this.tableName = featureGroup.getName() + "_" + featureGroup.getVersion();

    Map<String, String> hudiArgs = new HashMap<String, String>();

    hudiArgs.put(Constants.HUDI_TABLE_STORAGE_TYPE, Constants.HUDI_COPY_ON_WRITE);

    Seq<String> primaryColumns = utils.getPrimaryColumns(featureGroup);

    // TODO (davit) decide if you allow users to write feature group without primarykey
    if (primaryColumns.isEmpty()) {
      throw new FeatureStoreException("For time travel enabled feature groups You must provide at least 1 primary key");
    }

    hudiArgs.put(Constants.HUDI_RECORD_KEY, primaryColumns.mkString(","));

    // hudiArgs.put(Constants.HUDI_KEY_GENERATOR_OPT_KEY, Constants.HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL);

    Seq<String> partitionColumns = utils.getPartitionColumns(featureGroup);

    if (!partitionColumns.isEmpty()) {
      hudiArgs.put(Constants.HUDI_PARTITION_FIELD, partitionColumns.mkString(","));
      hudiArgs.put(Constants.HUDI_PRECOMBINE_FIELD, partitionColumns.mkString(","));
      hudiArgs.put(Constants.HUDI_HIVE_SYNC_PARTITION_FIELDS, partitionColumns.mkString(","));
    }

    hudiArgs.put(Constants.HUDI_TABLE_NAME, utils.getTableName(featureGroup));
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


  private Map<String, String> setupHudiReadArgs(FeatureGroup featureGroup, String startTime, String  endTime) {
    Map<String, String> hudiArgs = new HashMap<String, String>();
    Integer numberOfpartitionCols = utils.getPartitionColumns(featureGroup).length();
    this.basePath = utils.getHudiBasePath(featureGroup);
    this.tableName = featureGroup.getName() + "_" + featureGroup.getVersion();

    //Snapshot query
    if (startTime == null && endTime == null) {
      hudiArgs.put(Constants.HUDI_QUERY_TYPE_OPT_KEY, Constants.HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL);
      this.basePath =  this.basePath  + StringUtils.repeat("/*", numberOfpartitionCols + 1);
    } else if (endTime != null) {
      hudiArgs.put(Constants.HUDI_QUERY_TYPE_OPT_KEY, Constants.HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);
      //point in time  query
      if (startTime == null) {
        hudiArgs.put(Constants.HUDI_BEGIN_INSTANTTIME_OPT_KEY, "000");
        hudiArgs.put(Constants.HUDI_END_INSTANTTIME_OPT_KEY, endTime);
      } else if (startTime != null) {       //incremental  query
        hudiArgs.put(Constants.HUDI_BEGIN_INSTANTTIME_OPT_KEY, startTime);
        hudiArgs.put(Constants.HUDI_END_INSTANTTIME_OPT_KEY, endTime);
      }
    }

    return hudiArgs;
  }

  public Map<String, String> hudiReadArgs(FeatureGroup featureGroup, String startTime, String  endTime) {
    Map<String, String> hudiArgs = setupHudiReadArgs(featureGroup, startTime, endTime);
    return hudiArgs;
  }

  private void writeHudiDataset(SparkSession sparkSession, FeatureGroup featureGroup, Dataset<Row> dataset,
                                SaveMode saveMode, String operation) throws IOException, FeatureStoreException {

    Map<String, String> hudiArgs = setupHudiWriteArgs(featureGroup);

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

    // TODO (davit): make sure writing completed without exception
    FeatureGroupCommit featureGroupCommit = getLastCommitMetadata(sparkSession, this.basePath);
    featureGroupApi.featureCommit(featureGroup, featureGroupCommit);
  }

  public Map<Integer, String> getTimeLine(SparkSession sparkSession, String basePath) throws IOException {

    FileSystem hopsfsConf = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
    HoodieTimeline timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(hopsfsConf, basePath);


    Map<Integer, String> commitTimestamps = new HashMap<>();
    for (int i = 0; i < timeline.countInstants(); i++) {
      commitTimestamps.put(i, timeline.nthInstant(i).get().getTimestamp());
    }
    return commitTimestamps;
  }


  private FeatureGroupCommit getLastCommitMetadata(SparkSession sparkSession, String basePath)
      throws IOException {

    FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();

    FileSystem hopsfsConf = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
    HoodieTimeline commitTimeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(hopsfsConf, basePath);


    // TODO (davit): commit ID at the moment is nth instance.
    int commID = commitTimeline.countInstants();
    fgCommitMetadata.setCommitID(commID);
    fgCommitMetadata.setCommittedOn(commitTimeline.lastInstant().get().getTimestamp());
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


  public void writeTimeTravelEnabledFG(SparkSession sparkSession, FeatureGroup featureGroup, Dataset<Row> dataset,
                                       SaveMode saveMode, String operation)
      throws IOException, FeatureStoreException {

    writeHudiDataset(sparkSession, featureGroup, dataset, saveMode, operation);
  }

}
