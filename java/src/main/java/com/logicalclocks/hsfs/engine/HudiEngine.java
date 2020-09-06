package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.util.Constants;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import org.apache.hudi.HoodieDataSourceHelpers;

import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HudiEngine {
  private Utils utils = new Utils();

  private Map<String, String> setupHudiArgs(FeatureGroup featureGroup) throws IOException, FeatureStoreException {

    Map<String, String> hudiArgs = new HashMap<String, String>();

    hudiArgs.put(Constants.HUDI_TABLE_STORAGE_TYPE, Constants.HUDI_COPY_ON_WRITE);

    Seq<String> primaryColumns = utils.getPrimaryColumns(featureGroup);

    // TODO (davit) decide if you allow users to write feature group without primarykey
    if (primaryColumns.isEmpty()) {
      throw new FeatureStoreException("For time travel enabled feature groups You must provide at least 1 primary key");
    }

    hudiArgs.put(Constants.HUDI_RECORD_KEY, primaryColumns.mkString(","));

    Seq<String> partitionColumns = utils.getPartitionColumns(featureGroup);

    // TODO (davit): Decide what happens if particion key is not present
    if (!partitionColumns.isEmpty()) {
      hudiArgs.put(Constants.HUDI_PARTITION_FIELD, partitionColumns.mkString(","));
      hudiArgs.put(Constants.HUDI_PRECOMBINE_FIELD, partitionColumns.head());
      hudiArgs.put(Constants.HUDI_HIVE_SYNC_PARTITION_FIELDS, partitionColumns.mkString(","));
    }

    // TODO (davit): Decide what happens if key is not composite
    hudiArgs.put(Constants.HUDI_KEY_GENERATOR_OPT_KEY, Constants.HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL);

    hudiArgs.put(Constants.HUDI_TABLE_NAME, utils.getTableName(featureGroup));
    hudiArgs.put(Constants.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,
                Constants.DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL);

    // Hive
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_ENABLE, "true");
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_TABLE, utils.getTableName(featureGroup));
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


  private void writeHudiDataset(FeatureGroup featureGroup, Dataset<Row> dataset, SaveMode saveMode,
                                  String operation) throws IOException, FeatureStoreException {

    Map<String, String> hudiArgs = setupHudiArgs(featureGroup);

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
    writer.save(utils.getHudiBasePath(featureGroup));
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

  public void writeTimeTravelEnabledFG(FeatureGroup featureGroup, Dataset<Row> dataset, SaveMode saveMode,
                                       String operation) throws IOException, FeatureStoreException {

    writeHudiDataset(featureGroup, dataset, saveMode, operation);
  }

}
