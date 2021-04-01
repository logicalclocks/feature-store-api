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

import com.amazon.deequ.profiles.ColumnProfilerRunBuilder;
import com.amazon.deequ.profiles.ColumnProfilerRunner;
import com.amazon.deequ.profiles.ColumnProfiles;
import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.OnDemandFeatureGroup;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.OnDemandOptions;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.util.Constants;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.avro.functions.to_avro;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.struct;

import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkEngine {

  private static SparkEngine INSTANCE = null;

  public static synchronized SparkEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new SparkEngine();
    }
    return INSTANCE;
  }

  @Getter
  private SparkSession sparkSession;

  private Utils utils = new Utils();
  private HudiEngine hudiEngine = new HudiEngine();

  private SparkEngine() {
    sparkSession = SparkSession.builder()
        .enableHiveSupport()
        .getOrCreate();

    // Configure the Spark context to allow dynamic partitions
    sparkSession.conf().set("hive.exec.dynamic.partition", "true");
    sparkSession.conf().set("hive.exec.dynamic.partition.mode", "nonstrict");
    // force Spark to fallback to using the Hive Serde to read Hudi COPY_ON_WRITE tables
    sparkSession.conf().set("spark.sql.hive.convertMetastoreParquet", "false");
  }

  public String getTrustStorePath() {
    return sparkSession.conf().get("spark.hadoop.hops.ssl.trustore.name");
  }

  public String getKeyStorePath() {
    return sparkSession.conf().get("spark.hadoop.hops.ssl.keystore.name");
  }

  public String getCertKey() {
    return sparkSession.conf().get("spark.hadoop.hops.ssl.keystores.passwd.name");
  }

  public Dataset<Row> sql(String query) {
    return sparkSession.sql(query);
  }

  public Dataset<Row> jdbc(String query, StorageConnector storageConnector) throws FeatureStoreException {
    Map<String, String> readOptions = storageConnector.getSparkOptionsInt();
    if (!Strings.isNullOrEmpty(query)) {
      readOptions.put("query", query);
    }
    return sparkSession.read()
        .format(Constants.JDBC_FORMAT)
        .options(readOptions)
        .load();
  }

  public Dataset<Row> snowflake(String query, StorageConnector storageConnector) throws FeatureStoreException {
    Map<String, String> readOptions = storageConnector.getSparkOptionsInt();
    if (!Strings.isNullOrEmpty(query)) {
      readOptions.put("query", query);
    }
    return sparkSession.read()
        .format(Constants.SNOWFLAKE_FORMAT)
        .options(readOptions)
        .load();
  }

  public Dataset<Row> registerOnDemandTemporaryTable(OnDemandFeatureGroup onDemandFeatureGroup, String alias)
      throws FeatureStoreException {
    Dataset<Row> dataset;

    switch (onDemandFeatureGroup.getStorageConnector().getStorageConnectorType()) {
      case REDSHIFT:
      case JDBC:
        dataset = jdbc(onDemandFeatureGroup.getQuery(), onDemandFeatureGroup.getStorageConnector());
        break;
      case SNOWFLAKE:
        dataset = snowflake(onDemandFeatureGroup.getQuery(), onDemandFeatureGroup.getStorageConnector());
        break;
      default:
        dataset = read(onDemandFeatureGroup.getStorageConnector(),
            onDemandFeatureGroup.getDataFormat().toString(),
            getOnDemandOptions(onDemandFeatureGroup),
            onDemandFeatureGroup.getStorageConnector().getPath(onDemandFeatureGroup.getPath()));
    }
    dataset.createOrReplaceTempView(alias);
    return dataset;
  }

  private Map<String, String> getOnDemandOptions(OnDemandFeatureGroup onDemandFeatureGroup) {
    if (onDemandFeatureGroup.getOptions() == null) {
      return new HashMap<>();
    }

    return onDemandFeatureGroup.getOptions().stream()
        .collect(Collectors.toMap(OnDemandOptions::getName, OnDemandOptions::getValue));
  }

  public void registerHudiTemporaryTable(FeatureGroup featureGroup, String alias, Long leftFeaturegroupStartTimestamp,
                                         Long leftFeaturegroupEndTimestamp, Map<String, String> readOptions) {
    hudiEngine.registerTemporaryTable(sparkSession, featureGroup, alias,
        leftFeaturegroupStartTimestamp, leftFeaturegroupEndTimestamp, readOptions);
  }

  /**
   * Setup Spark to write the data on the File System.
   *
   * @param trainingDataset
   * @param dataset
   * @param writeOptions
   * @param saveMode
   */
  public void write(TrainingDataset trainingDataset, Dataset<Row> dataset,
                    Map<String, String> writeOptions, SaveMode saveMode) {

    setupConnectorHadoopConf(trainingDataset.getStorageConnector());

    if (trainingDataset.getCoalesce()) {
      dataset = dataset.coalesce(1);
    }

    if (trainingDataset.getSplits() == null) {
      // Write a single dataset

      // The actual data will be stored in training_ds_version/training_ds the double directory is needed
      // for cases such as tfrecords in which we need to store also the schema
      // also in case of multiple splits, the single splits will be stored inside the training dataset dir
      String path = new Path(trainingDataset.getLocation(), trainingDataset.getName()).toString();
      writeSingle(dataset, trainingDataset.getDataFormat(),
          writeOptions, saveMode, path);
    } else {
      List<Float> splitFactors = trainingDataset.getSplits().stream()
          .map(Split::getPercentage)
          .collect(Collectors.toList());

      // The actual data will be stored in training_ds_version/split_name
      Dataset<Row>[] datasetSplits = null;
      if (trainingDataset.getSeed() != null) {
        datasetSplits = dataset.randomSplit(
            splitFactors.stream().mapToDouble(Float::doubleValue).toArray(), trainingDataset.getSeed());
      } else {
        datasetSplits = dataset.randomSplit(splitFactors.stream().mapToDouble(Float::doubleValue).toArray());
      }

      writeSplits(datasetSplits,
          trainingDataset.getDataFormat(), writeOptions, saveMode,
          trainingDataset.getLocation(), trainingDataset.getSplits());
    }
  }

  public Map<String, String> getWriteOptions(Map<String, String> providedOptions, DataFormat dataFormat) {
    Map<String, String> writeOptions = new HashMap<>();
    switch (dataFormat) {
      case CSV:
        writeOptions.put(Constants.HEADER, "true");
        writeOptions.put(Constants.DELIMITER, ",");
        break;
      case TSV:
        writeOptions.put(Constants.HEADER, "true");
        writeOptions.put(Constants.DELIMITER, "\t");
        break;
      case TFRECORDS:
      case TFRECORD:
        writeOptions.put(Constants.TF_CONNECTOR_RECORD_TYPE, "Example");
        break;
      default:
        break;
    }

    if (providedOptions != null && !providedOptions.isEmpty()) {
      writeOptions.putAll(providedOptions);
    }
    return writeOptions;
  }

  public Map<String, String> getReadOptions(Map<String, String> providedOptions, DataFormat dataFormat) {
    Map<String, String> readOptions = new HashMap<>();
    switch (dataFormat) {
      case CSV:
        readOptions.put(Constants.HEADER, "true");
        readOptions.put(Constants.DELIMITER, ",");
        readOptions.put(Constants.INFER_SCHEMA, "true");
        break;
      case TSV:
        readOptions.put(Constants.HEADER, "true");
        readOptions.put(Constants.DELIMITER, "\t");
        readOptions.put(Constants.INFER_SCHEMA, "true");
        break;
      case TFRECORDS:
      case TFRECORD:
        readOptions.put(Constants.TF_CONNECTOR_RECORD_TYPE, "Example");
        break;
      default:
        break;
    }
    if (providedOptions != null && !providedOptions.isEmpty()) {
      readOptions.putAll(providedOptions);
    }
    return readOptions;
  }

  /**
   * Write multiple training dataset splits and name them.
   *
   * @param datasets
   * @param dataFormat
   * @param writeOptions
   * @param saveMode
   * @param basePath
   * @param splits
   */
  private void writeSplits(Dataset<Row>[] datasets, DataFormat dataFormat, Map<String, String> writeOptions,
                           SaveMode saveMode, String basePath, List<Split> splits) {
    for (int i = 0; i < datasets.length; i++) {
      writeSingle(datasets[i], dataFormat, writeOptions, saveMode,
          new Path(basePath, splits.get(i).getName()).toString());
    }
  }

  /**
   * Write a single dataset split.
   *
   * @param dataset
   * @param dataFormat
   * @param writeOptions
   * @param saveMode
   * @param path         it should be the full path
   */
  private void writeSingle(Dataset<Row> dataset, DataFormat dataFormat,
                           Map<String, String> writeOptions, SaveMode saveMode, String path) {
    dataset
        .write()
        .format(dataFormat.toString())
        .options(writeOptions)
        .mode(saveMode)
        .save(SparkEngine.sparkPath(path));
  }

  // Here dataFormat is string as this method is used both with OnDemand Feature Groups as well as with
  // Training Dataset. They use 2 different enumerators for dataFormat, as for instance, we don't allow
  // OnDemand Feature Group in TFRecords format. However Spark does not use an enum but a string.
  public Dataset<Row> read(StorageConnector storageConnector, String dataFormat,
                           Map<String, String> readOptions, String path) {
    setupConnectorHadoopConf(storageConnector);

    return SparkEngine.getInstance().getSparkSession()
        .read()
        .format(dataFormat)
        .options(readOptions)
        .load(SparkEngine.sparkPath(path));
  }

  /**
   * Writes feature group dataframe to kafka for online-fs ingestion.
   *
   * @param featureGroup
   * @param dataset
   * @param writeOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void writeOnlineDataframe(FeatureGroup featureGroup, Dataset<Row> dataset, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    onlineFeatureGroupToAvro(featureGroup, encodeComplexFeatures(featureGroup, dataset))
        .write()
        .format(Constants.KAFKA_FORMAT)
        .options(writeOptions)
        .option("topic", featureGroup.getOnlineTopicName())
        .save();
  }

  public StreamingQuery writeStreamDataframe(FeatureGroup featureGroup, Dataset<Row> dataset, String queryName,
                                             String outputMode, boolean awaitTermination, Long timeout,
                                             Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, StreamingQueryException {

    if (Strings.isNullOrEmpty(queryName)) {
      queryName = "insert_stream_" + featureGroup.getOnlineTopicName() + "_" + LocalDateTime.now().format(
          DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }

    DataStreamWriter<Row> writer = onlineFeatureGroupToAvro(featureGroup, encodeComplexFeatures(featureGroup, dataset))
        .writeStream()
        .format(Constants.KAFKA_FORMAT)
        .outputMode(outputMode)
        .options(writeOptions)
        .option("checkpointLocation", "/Projects/" + HopsworksClient.getInstance().getProject() + "/Resources/"
            + queryName + "-checkpoint")
        .option("topic", featureGroup.getOnlineTopicName());

    StreamingQuery query = writer.start();
    if (awaitTermination) {
      query.awaitTermination(timeout);
    }
    return query;
  }

  /**
   * Encodes all complex type features to binary using their avro type as schema.
   *
   * @param featureGroup
   * @param dataset
   * @return
   */
  public Dataset<Row> encodeComplexFeatures(FeatureGroup featureGroup, Dataset<Row> dataset)
      throws FeatureStoreException, IOException {
    List<Column> select = new ArrayList<>();
    for (Schema.Field f : featureGroup.getDeserializedAvroSchema().getFields()) {
      if (featureGroup.getComplexFeatures().contains(f.name())) {
        select.add(to_avro(col(f.name()), featureGroup.getFeatureAvroSchema(f.name())).alias(f.name()));
      } else {
        select.add(col(f.name()));
      }
    }
    return dataset.select(select.stream().toArray(Column[]::new));
  }

  /**
   * Serializes dataframe to two binary columns, one avro serialized key and one avro serialized value column.
   *
   * @param featureGroup
   * @param dataset
   * @return dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  private Dataset<Row> onlineFeatureGroupToAvro(FeatureGroup featureGroup, Dataset<Row> dataset)
      throws FeatureStoreException, IOException {
    Collections.sort(featureGroup.getPrimaryKeys());
    return dataset.select(
        to_avro(concat(featureGroup.getPrimaryKeys().stream().map(name -> col(name).cast("string"))
            .toArray(Column[]::new))).alias("key"),
        to_avro(struct(featureGroup.getDeserializedAvroSchema().getFields().stream()
                .map(f -> col(f.name())).toArray(Column[]::new)), featureGroup.getEncodedAvroSchema()).alias("value"));
  }

  public void writeOfflineDataframe(FeatureGroup featureGroup, Dataset<Row> dataset,
                                    HudiOperationType operation, Map<String, String> writeOptions, Integer validationId)
      throws IOException, FeatureStoreException, ParseException {

    if (featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI) {
      hudiEngine.saveHudiFeatureGroup(sparkSession, featureGroup, dataset, operation, writeOptions, validationId);
    } else {
      writeSparkDataset(featureGroup, dataset, writeOptions);
    }
  }

  private void writeSparkDataset(FeatureGroup featureGroup, Dataset<Row> dataset, Map<String, String> writeOptions) {
    dataset
        .write()
        .format(Constants.HIVE_FORMAT)
        .mode(SaveMode.Append)
        // write options cannot be null
        .options(writeOptions == null ? new HashMap<>() : writeOptions)
        .partitionBy(utils.getPartitionColumns(featureGroup))
        .saveAsTable(utils.getTableName(featureGroup));
  }

  public String profile(Dataset<Row> df, List<String> restrictToColumns, Boolean correlation, Boolean histogram) {
    // only needed for training datasets, as the backend is not setting the defaults
    if (correlation == null) {
      correlation = true;
    }
    if (histogram == null) {
      histogram = true;
    }
    ColumnProfilerRunBuilder runner =
        new ColumnProfilerRunner().onData(df).withCorrelation(correlation).withHistogram(histogram);
    if (restrictToColumns != null && !restrictToColumns.isEmpty()) {
      runner.restrictToColumns(JavaConverters.asScalaIteratorConverter(restrictToColumns.iterator()).asScala().toSeq());
    }
    ColumnProfiles result = runner.run();
    return ColumnProfiles.toJson(result.profiles().values().toSeq());
  }

  public String profile(Dataset<Row> df, List<String> restrictToColumns) {
    return profile(df, restrictToColumns, true, true);
  }

  public String profile(Dataset<Row> df, boolean correlation, boolean histogram) {
    return profile(df, null, correlation, histogram);
  }

  public String profile(Dataset<Row> df) {
    return profile(df, null, true, true);
  }

  public void setupConnectorHadoopConf(StorageConnector storageConnector) {
    if (storageConnector == null) {
      return;
    }

    switch (storageConnector.getStorageConnectorType()) {
      case S3:
        setupS3ConnectorHadoopConf(storageConnector);
        break;
      case ADLS:
        setupAdlsConnectorHadoopConf(storageConnector);
        break;
      default:
        // No-OP
        break;
    }
  }

  public static String sparkPath(String path) {
    if (path.startsWith(Constants.S3_SCHEME)) {
      return path.replaceFirst(Constants.S3_SCHEME, Constants.S3_SPARK_SCHEME);
    }
    return path;
  }

  private void setupS3ConnectorHadoopConf(StorageConnector storageConnector) {
    if (!Strings.isNullOrEmpty(storageConnector.getAccessKey())) {
      sparkSession.sparkContext().hadoopConfiguration()
          .set(Constants.S3_ACCESS_KEY_ENV, storageConnector.getAccessKey());
    }
    if (!Strings.isNullOrEmpty(storageConnector.getSecretKey())) {
      sparkSession.sparkContext().hadoopConfiguration()
          .set(Constants.S3_SECRET_KEY_ENV, storageConnector.getSecretKey());
    }
    if (!Strings.isNullOrEmpty(storageConnector.getServerEncryptionAlgorithm())) {
      sparkSession.sparkContext().hadoopConfiguration().set(
          "fs.s3a.server-side-encryption-algorithm",
          storageConnector.getServerEncryptionAlgorithm()
      );
    }
    if (!Strings.isNullOrEmpty(storageConnector.getServerEncryptionKey())) {
      sparkSession.sparkContext().hadoopConfiguration()
          .set("fs.s3a.server-side-encryption.key", storageConnector.getServerEncryptionKey());
    }
    if (!Strings.isNullOrEmpty(storageConnector.getSessionToken())) {
      sparkSession.sparkContext().hadoopConfiguration()
          .set(Constants.S3_CREDENTIAL_PROVIDER_ENV, Constants.S3_TEMPORARY_CREDENTIAL_PROVIDER);
      sparkSession.sparkContext().hadoopConfiguration()
          .set(Constants.S3_SESSION_KEY_ENV, storageConnector.getSessionToken());
    }
  }

  private void setupAdlsConnectorHadoopConf(StorageConnector storageConnector) {
    for (Option confOption : storageConnector.getSparkOptions()) {
      sparkSession.sparkContext().hadoopConfiguration().set(confOption.getName(), confOption.getValue());
    }
  }
}
