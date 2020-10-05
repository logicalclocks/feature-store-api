package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.util.Constants;

import lombok.Getter;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


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

public class HudiFeatureGroupEngine extends FeatureGroupBaseEngine  {

  @Getter
  private String basePath;
  @Getter
  private String tableName;


  private Utils utils = new Utils();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

  private Map<String, String> setupHudiWriteArgs(FeatureGroup featureGroup) throws IOException, FeatureStoreException {

    this.basePath = featureGroup.getLocation();
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

  private Map<String, String> setupHudiReadArgs(FeatureGroup featureGroup, String wallclockStartTime,
                                                String wallclockEndTime) throws IOException, FeatureStoreException {
    Map<String, String> hudiArgs = new HashMap<String, String>();
    Integer numberOfpartitionCols = utils.getPartitionColumns(featureGroup).length();
    this.basePath = featureGroup.getLocation();

    //Snapshot query
    if (wallclockStartTime == null && wallclockEndTime == null) {
      hudiArgs.put(Constants.HUDI_QUERY_TYPE_OPT_KEY, Constants.HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL);
      this.basePath =  this.basePath  + StringUtils.repeat("/*", numberOfpartitionCols + 1);
    } else if (wallclockEndTime != null) {
      hudiArgs.put(Constants.HUDI_QUERY_TYPE_OPT_KEY, Constants.HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);
      Long endTimeStamp = hudiCommitToTimeStamp(wallclockEndTime);
      FeatureGroupCommit endCommit = featureGroupApi.pointInTimeCommitDetails(featureGroup, endTimeStamp);
      String hudiCommitEndTime = timeStampToHudiFormat(endCommit.getCommittime());
      //point in time  query
      if (wallclockStartTime == null) {
        hudiArgs.put(Constants.HUDI_BEGIN_INSTANTTIME_OPT_KEY, "000");
        hudiArgs.put(Constants.HUDI_END_INSTANTTIME_OPT_KEY, hudiCommitEndTime);
      } else if (wallclockStartTime != null) {       //incremental  query
        Long startTimeStamp = hudiCommitToTimeStamp(wallclockStartTime);
        FeatureGroupCommit startCommit = featureGroupApi.pointInTimeCommitDetails(featureGroup, startTimeStamp);
        String hudiCommitStartTime = timeStampToHudiFormat(startCommit.getCommittime());
        hudiArgs.put(Constants.HUDI_BEGIN_INSTANTTIME_OPT_KEY, hudiCommitStartTime);
        hudiArgs.put(Constants.HUDI_END_INSTANTTIME_OPT_KEY, wallclockEndTime);
      }
    }

    return hudiArgs;
  }

  private Map<String, String> hudiReadArgs(FeatureGroup featureGroup, String wallclockStartTime,
                                           String wallclockEndTime) throws IOException, FeatureStoreException {
    Map<String, String> hudiArgs = setupHudiReadArgs(featureGroup, wallclockStartTime, wallclockEndTime);
    return hudiArgs;
  }

  private FeatureGroupCommit writeHudiDataset(SparkSession sparkSession, FeatureGroup featureGroup,
                                              Dataset<Row> dataset, SaveMode saveMode, String operation)
      throws IOException, FeatureStoreException {

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

    FeatureGroupCommit featureGroupCommit = getLastCommitMetadata(sparkSession, this.basePath);
    return featureGroupCommit;
  }

  //hudi time time travel sql query
  public void registerTemporaryTable(SparkSession sparkSession, FeatureGroup featureGroup, String alias,
                                     String wallclockStartTime, String  wallclockEndTime)
      throws IOException, FeatureStoreException {

    sparkSession.conf().set("spark.sql.hive.convertMetastoreParquet", "false");
    sparkSession.sparkContext().hadoopConfiguration().setClass("mapreduce.input.pathFilter.class",
        org.apache.hudi.hadoop.HoodieROTablePathFilter.class, org.apache.hadoop.fs.PathFilter.class);

    Map<String, String> hudiArgs = hudiReadArgs(featureGroup, wallclockStartTime, wallclockEndTime);

    DataFrameReader queryDataset = sparkSession.read().format(Constants.HUDI_SPARK_FORMAT);
    for (Map.Entry<String, String> entry : hudiArgs.entrySet()) {
      queryDataset = queryDataset.option(entry.getKey(), entry.getValue());
    }

    queryDataset.load(getBasePath()).registerTempTable(alias);
  }



  private FeatureGroupCommit getLastCommitMetadata(SparkSession sparkSession, String basePath)
      throws IOException {

    FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();

    FileSystem hopsfsConf = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
    HoodieTimeline commitTimeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(hopsfsConf, basePath);

    Long commitTimeStamp = hudiCommitToTimeStamp(commitTimeline.lastInstant().get().getTimestamp());
    fgCommitMetadata.setCommittime(commitTimeStamp);
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

  public void saveHudiFeatureGroup(SparkSession sparkSession, FeatureGroup featureGroup, Dataset<Row> dataset,
                                   SaveMode saveMode, String operation)
      throws IOException, FeatureStoreException {

    // TODO (davit): make sure writing completed without exception
    FeatureGroupCommit fgCommit = writeHudiDataset(sparkSession, featureGroup, dataset, saveMode, operation);

    FeatureGroupCommit apiFgCommit = featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
    apiFgCommit.setCommitID(apiFgCommit.getCommitID());

  }

  @SneakyThrows
  public Long hudiCommitToTimeStamp(String hudiCommitTime) {
    // TODO (davit): we need to have util fucntion that will accepted date formats and convert to hudi spec dateformat
    Long commitTimeStamp = dateFormat.parse(hudiCommitTime).getTime();
    return commitTimeStamp;
  }

  @SneakyThrows
  private String timeStampToHudiFormat(Long commitedOnTimeStamp) {
    Date commitedOnDate = new Timestamp(commitedOnTimeStamp);
    String strCommitedOnDate = dateFormat.format(commitedOnDate);
    return strCommitedOnDate;
  }

  // TODO (davit): move to backend
  public List<Feature> addHudiSpecFeatures(List<Feature> features) throws FeatureStoreException {
    features.add(new Feature("_hoodie_record_key", "string",
        "string", false, false, false));
    features.add(new Feature("_hoodie_partition_path", "string",
        "string", false, false, false));
    features.add(new Feature("_hoodie_commit_time", "string",
        "string", false, false, false));
    features.add(new Feature("_hoodie_file_name", "string",
        "string", false, false, false));
    features.add(new Feature("_hoodie_commit_seqno", "string",
        "string", false, false, false));
    return features;
  }

  // TODO (davit): move to backend
  public Dataset<Row>  dropHudiSpecFeatures(Dataset<Row> dataset) {
    return  dataset.drop("_hoodie_record_key", "_hoodie_partition_path", "_hoodie_commit_time",
        "_hoodie_file_name", "_hoodie_commit_seqno");
  }


}
