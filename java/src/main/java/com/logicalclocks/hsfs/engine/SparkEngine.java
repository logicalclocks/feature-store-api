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
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.OnDemandFeatureGroup;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.engine.hudi.HudiEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.OnDemandOptions;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.util.Constants;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.avro.functions.to_avro;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

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

  private FeatureGroupUtils utils = new FeatureGroupUtils();
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

  public void validateSparkConfiguration() throws FeatureStoreException {
    String exceptionText = "Spark is misconfigured for communication with Hopsworks, missing or invalid property: ";

    Map<String, String> configurationMap = new HashMap<>();
    configurationMap.put("spark.hadoop.hops.ssl.trustore.name", null);
    configurationMap.put("spark.hadoop.hops.rpc.socket.factory.class.default",
            "io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory");
    configurationMap.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    configurationMap.put("spark.hadoop.hops.ssl.hostname.verifier", "ALLOW_ALL");
    configurationMap.put("spark.hadoop.hops.ssl.keystore.name", null);
    configurationMap.put("spark.hadoop.fs.hopsfs.impl", "io.hops.hopsfs.client.HopsFileSystem");
    configurationMap.put("spark.hadoop.hops.ssl.keystores.passwd.name", null);
    configurationMap.put("spark.hadoop.hops.ipc.server.ssl.enabled", "true");
    configurationMap.put("spark.sql.hive.metastore.jars", null);
    configurationMap.put("spark.hadoop.client.rpc.ssl.enabled.protocol", "TLSv1.2");
    configurationMap.put("spark.hadoop.hive.metastore.uris", null);

    for (Map.Entry<String, String> entry : configurationMap.entrySet()) {
      if (!(sparkSession.conf().contains(entry.getKey())
              && (entry.getValue() == null
              || sparkSession.conf().get(entry.getKey(), null).equals(entry.getValue())))) {
        throw new FeatureStoreException(exceptionText + entry.getKey());
      }
    }
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

  public Dataset<Row> registerOnDemandTemporaryTable(OnDemandFeatureGroup onDemandFeatureGroup, String alias)
      throws FeatureStoreException, IOException {
    Dataset<Row> dataset = (Dataset<Row>) onDemandFeatureGroup.getStorageConnector()
        .read(onDemandFeatureGroup.getQuery(),
        onDemandFeatureGroup.getDataFormat() != null ? onDemandFeatureGroup.getDataFormat().toString() : null,
        getOnDemandOptions(onDemandFeatureGroup),
        onDemandFeatureGroup.getStorageConnector().getPath(onDemandFeatureGroup.getPath()));
    if (!Strings.isNullOrEmpty(onDemandFeatureGroup.getLocation())) {
      sparkSession.sparkContext().textFile(onDemandFeatureGroup.getLocation(), 0).collect();
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

  public void registerHudiTemporaryTable(FeatureGroupBase featureGroup, String alias,
                                         Long leftFeaturegroupStartTimestamp,
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
  public Dataset<Row>[] write(TrainingDataset trainingDataset, Dataset<Row> dataset,
                    Map<String, String> writeOptions, SaveMode saveMode) throws FeatureStoreException, IOException {
    setupConnectorHadoopConf(trainingDataset.getStorageConnector());

    if (trainingDataset.getCoalesce()) {
      dataset = dataset.coalesce(1);
    }

    if (trainingDataset.getSplits() == null || trainingDataset.getSplits().isEmpty()) {
      // Write a single dataset

      // The actual data will be stored in training_ds_version/training_ds the double directory is needed
      // for cases such as tfrecords in which we need to store also the schema
      // also in case of multiple splits, the single splits will be stored inside the training dataset dir
      String path = new Path(trainingDataset.getLocation(), trainingDataset.getName()).toString();
      writeSingle(dataset, trainingDataset.getDataFormat(),
          writeOptions, saveMode, path);
      return new Dataset[] {dataset};
    } else {
      Dataset<Row>[] datasetSplits = splitDataset(trainingDataset, dataset);
      writeSplits(datasetSplits,
          trainingDataset.getDataFormat(), writeOptions, saveMode,
          trainingDataset.getLocation(), trainingDataset.getSplits());
      return datasetSplits;
    }
  }

  public Dataset<Row>[] splitDataset(TrainingDataset trainingDataset, Dataset<Row> dataset) {
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
    return datasetSplits;
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
                           Map<String, String> readOptions, String location) throws FeatureStoreException, IOException {
    setupConnectorHadoopConf(storageConnector);

    String path = "";
    if (location != null) {
      path = new Path(location, "**").toString();
    } else {
      // path is null for jdbc kind of on demand fgs
      path = null;
    }
    path = SparkEngine.sparkPath(path);

    DataFrameReader reader = SparkEngine.getInstance().getSparkSession()
        .read()
        .format(dataFormat)
        .options(readOptions);

    if (!Strings.isNullOrEmpty(path)) {
      // for BigQuery we set SQL query as location which should be passed to load()
      if (dataFormat.equals(Constants.BIGQUERY_FORMAT)) {
        return reader.load(location);
      }
      return reader.load(SparkEngine.sparkPath(path));
    }
    return reader.load();
  }

  /**
   * Writes feature group dataframe to kafka for online-fs ingestion.
   *
   * @param featureGroupBase
   * @param dataset
   * @param writeOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public <S> void writeOnlineDataframe(FeatureGroupBase featureGroupBase, S dataset, String onlineTopicName,
                                         Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    onlineFeatureGroupToAvro(featureGroupBase, encodeComplexFeatures(featureGroupBase, (Dataset<Row>) dataset))
        .write()
        .format(Constants.KAFKA_FORMAT)
        .options(writeOptions)
        .option("topic", onlineTopicName)
        .save();
  }

  public <S> StreamingQuery writeStreamDataframe(FeatureGroupBase featureGroupBase, S datasetGeneric, String queryName,
                                             String outputMode, boolean awaitTermination, Long timeout,
                                             String checkpointLocation, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException {

    Dataset<Row> dataset = (Dataset<Row>) datasetGeneric;
    DataStreamWriter<Row> writer =
        onlineFeatureGroupToAvro(featureGroupBase, encodeComplexFeatures(featureGroupBase, dataset))
        .writeStream()
        .format(Constants.KAFKA_FORMAT)
        .outputMode(outputMode)
        .option("checkpointLocation", checkpointLocation == null
            ? utils.checkpointDirPath(queryName, featureGroupBase.getOnlineTopicName())
            : checkpointLocation)
        .options(writeOptions)
        .option("topic", featureGroupBase.getOnlineTopicName());

    // start streaming to online feature group topic
    StreamingQuery query = writer.start();
    if (awaitTermination) {
      query.awaitTermination(timeout);
    }
    return query;
  }

  /**
   * Encodes all complex type features to binary using their avro type as schema.
   *
   * @param featureGroupBase
   * @param dataset
   * @return
   */
  public Dataset<Row> encodeComplexFeatures(FeatureGroupBase featureGroupBase, Dataset<Row> dataset)
          throws FeatureStoreException, IOException {

    List<Column> select = new ArrayList<>();
    for (Schema.Field f : featureGroupBase.getDeserializedAvroSchema().getFields()) {
      if (featureGroupBase.getComplexFeatures().contains(f.name())) {
        select.add(to_avro(col(f.name()), featureGroupBase.getFeatureAvroSchema(f.name())).alias(f.name()));
      } else {
        select.add(col(f.name()));
      }
    }
    return dataset.select(select.stream().toArray(Column[]::new));
  }

  /**
   * Serializes dataframe to two binary columns, one avro serialized key and one avro serialized value column.
   *
   * @param featureGroupBase
   * @param dataset
   * @return dataset
   * @throws FeatureStoreException
   * @throws IOException
   */
  private Dataset<Row> onlineFeatureGroupToAvro(FeatureGroupBase featureGroupBase, Dataset<Row> dataset)
      throws FeatureStoreException, IOException {
    return dataset.select(
        to_avro(concat(featureGroupBase.getPrimaryKeys().stream().map(name -> col(name).cast("string"))
            .toArray(Column[]::new))).alias("key"),
        to_avro(struct(featureGroupBase.getDeserializedAvroSchema().getFields().stream()
                .map(f -> col(f.name())).toArray(Column[]::new)),
            featureGroupBase.getEncodedAvroSchema()).alias("value"));
  }

  public <S>  void writeOfflineDataframe(StreamFeatureGroup streamFeatureGroup, S genericDataset,
      HudiOperationType operation, Map<String, String> writeOptions, Integer validationId)
      throws IOException, FeatureStoreException, ParseException {

    Dataset<Row> dataset = (Dataset<Row>) genericDataset;
    hudiEngine.saveHudiFeatureGroup(sparkSession, streamFeatureGroup, dataset, operation, writeOptions, validationId);
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

  public String profile(Dataset<Row> df, List<String> restrictToColumns, Boolean correlation,
      Boolean histogram, Boolean exactUniqueness) {
    // only needed for training datasets, as the backend is not setting the defaults
    if (correlation == null) {
      correlation = true;
    }
    if (histogram == null) {
      histogram = true;
    }
    if (exactUniqueness == null) {
      exactUniqueness = true;
    }
    ColumnProfilerRunBuilder runner = new ColumnProfilerRunner()
                                            .onData(df)
                                            .withCorrelation(correlation, 100)
                                            .withHistogram(histogram, 20)
                                            .withExactUniqueness(exactUniqueness);
    if (restrictToColumns != null && !restrictToColumns.isEmpty()) {
      runner.restrictToColumns(JavaConverters.asScalaIteratorConverter(restrictToColumns.iterator()).asScala().toSeq());
    }
    ColumnProfiles result = runner.run();
    return ColumnProfiles.toJson(result.profiles().values().toSeq(), result.numRecords());
  }

  public String profile(Dataset<Row> df, List<String> restrictToColumns, Boolean correlation, Boolean histogram) {
    return profile(df, restrictToColumns, correlation, histogram, true);
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

  public void setupConnectorHadoopConf(StorageConnector storageConnector) throws FeatureStoreException, IOException {
    if (storageConnector == null) {
      return;
    }

    // update connector to get new session token
    storageConnector.refetch();

    switch (storageConnector.getStorageConnectorType()) {
      case S3:
        setupS3ConnectorHadoopConf((StorageConnector.S3Connector) storageConnector);
        break;
      case ADLS:
        setupAdlsConnectorHadoopConf((StorageConnector.AdlsConnector) storageConnector);
        break;
      case GCS:
        setupGcsConnectorHadoopConf((StorageConnector.GcsConnector) storageConnector);
        break;
      default:
        // No-OP
        break;
    }
  }

  public static String sparkPath(String path) {
    if (path == null) {
      return null;
    } else if (path.startsWith(Constants.S3_SCHEME)) {
      return path.replaceFirst(Constants.S3_SCHEME, Constants.S3_SPARK_SCHEME);
    }
    return path;
  }

  private void setupS3ConnectorHadoopConf(StorageConnector.S3Connector storageConnector) {
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
          .set("fs.s3a.server-side-encryption-key", storageConnector.getServerEncryptionKey());
    }
    if (!Strings.isNullOrEmpty(storageConnector.getSessionToken())) {
      sparkSession.sparkContext().hadoopConfiguration()
          .set(Constants.S3_CREDENTIAL_PROVIDER_ENV, Constants.S3_TEMPORARY_CREDENTIAL_PROVIDER);
      sparkSession.sparkContext().hadoopConfiguration()
          .set(Constants.S3_SESSION_KEY_ENV, storageConnector.getSessionToken());
    }
  }

  private void setupAdlsConnectorHadoopConf(StorageConnector.AdlsConnector storageConnector) {
    for (Option confOption : storageConnector.getSparkOptions()) {
      sparkSession.sparkContext().hadoopConfiguration().set(confOption.getName(), confOption.getValue());
    }
  }

  public Dataset<Row> getEmptyAppendedDataframe(Dataset<Row> dataframe, List<Feature> newFeatures) {
    Dataset<Row> emptyDataframe = dataframe.limit(0);
    for (Feature f : newFeatures) {
      emptyDataframe = emptyDataframe.withColumn(f.getName(), lit(null).cast(f.getType()));
    }
    return emptyDataframe;
  }

  public void streamToHudiTable(StreamFeatureGroup streamFeatureGroup, Map<String, String> writeOptions)
      throws Exception {
    writeOptions = utils.getKafkaConfig(streamFeatureGroup, writeOptions);
    hudiEngine.streamToHoodieTable(sparkSession, streamFeatureGroup, writeOptions);
  }

  public <S> List<Feature> parseFeatureGroupSchema(S datasetGeneric) throws FeatureStoreException {
    List<Feature> features = new ArrayList<>();
    Dataset<Row> dataset = (Dataset<Row>) datasetGeneric;
    for (StructField structField : dataset.schema().fields()) {
      // TODO(Fabio): unit test this one for complext types
      Feature f = new Feature(structField.name().toLowerCase(), structField.dataType().catalogString(), false, false);
      if (structField.metadata().contains("description")) {
        f.setDescription(structField.metadata().getString("description"));
      }
      features.add(f);
    }

    return features;
  }

  public <S> S sanitizeFeatureNames(S datasetGeneric) {
    Dataset<Row> dataset = (Dataset<Row>) datasetGeneric;
    return (S) dataset.select(Arrays.asList(dataset.columns()).stream().map(f -> col(f).alias(f.toLowerCase())).toArray(
        Column[]::new));
  }

  public String addFile(String filePath) {
    sparkSession.sparkContext().addFile("hdfs://" + filePath);
    return SparkFiles.get((new Path(filePath)).getName());
  }

  public Dataset<Row> readStream(StorageConnector storageConnector, String dataFormat, String messageFormat,
                                 String schema, Map<String, String> options, boolean includeMetadata)
      throws FeatureStoreException {
    DataStreamReader stream = sparkSession.readStream().format(dataFormat);

    // set user options last so that they overwrite any default options
    stream = stream.options(storageConnector.sparkOptions()).options(options);

    if (storageConnector instanceof StorageConnector.KafkaConnector) {
      return readStreamKafka(stream, messageFormat, schema, includeMetadata);
    }
    throw new FeatureStoreException("Connector does not support reading data into stream.");
  }

  private Dataset<Row> readStreamKafka(DataStreamReader stream, String messageFormat, String schema,
      boolean includeMetadata) throws SchemaParseException, FeatureStoreException {
    Column[] kafkaMetadataColumns = Arrays.asList(
        col("key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType"),
        col("value.*")
    ).toArray(new Column[7]);

    if (messageFormat.equals("avro") && !Strings.isNullOrEmpty(schema)) {
      Schema.Parser parser = new Schema.Parser();
      parser.parse(schema);
      Dataset<Row> df = stream.load();

      if (includeMetadata) {
        return df.withColumn("value", from_avro(df.col("value"), schema))
          .select(kafkaMetadataColumns);
      }
      return df.withColumn("value", from_avro(df.col("value"), schema)).select(col("value.*"));
    } else if (messageFormat.equals("json") && !Strings.isNullOrEmpty(schema)) {
      Dataset<Row> df = stream.load();

      if (includeMetadata) {
        return df.withColumn("value", from_json(df.col("value").cast("string"),
          schema, new HashMap<>()))
          .select(kafkaMetadataColumns);
      }
      return df.withColumn("value", from_json(df.col("value").cast("string"), schema, new HashMap<>()))
        .select(col("value.*"));
    }

    if (includeMetadata) {
      return stream.load();
    }
    return stream.load().select("key", "value");
  }

  public Dataset<Row> objectToDataset(Object obj) {
    return (Dataset<Row>) obj;
  }

  private void setupGcsConnectorHadoopConf(StorageConnector.GcsConnector storageConnector) {
    // The AbstractFileSystem for 'gs:' URIs
    sparkSession.sparkContext().hadoopConfiguration().set(
        Constants.PROPERTY_GCS_FS_KEY, Constants.PROPERTY_GCS_FS_VALUE
    );
    // Whether to use a service account for GCS authorization. Setting this
    // property to `false` will disable use of service accounts for authentication.
    sparkSession.sparkContext().hadoopConfiguration().set(
        Constants.PROPERTY_GCS_ACCOUNT_ENABLE, "true"
    );
    // The JSON key file of the service account used for GCS
    // access when google.cloud.auth.service.account.enable is true.
    String localPath = addFile(storageConnector.getKeyPath());
    sparkSession.sparkContext().hadoopConfiguration().set(
        Constants.PROPERTY_KEY_FILE, localPath
    );

    // if encryption fields present
    if (!Strings.isNullOrEmpty(storageConnector.getAlgorithm())) {
      sparkSession.sparkContext().hadoopConfiguration().set(
          Constants.PROPERTY_ALGORITHM, storageConnector.getAlgorithm());
      sparkSession.sparkContext().hadoopConfiguration().set(
          Constants.PROPERTY_ENCRYPTION_KEY, storageConnector.getEncryptionKey());
      sparkSession.sparkContext().hadoopConfiguration().set(
          Constants.PROPERTY_ENCRYPTION_HASH, storageConnector.getEncryptionKeyHash());
    } else {
      // unset if set before
      sparkSession.sparkContext().hadoopConfiguration().unset(Constants.PROPERTY_ALGORITHM);
      sparkSession.sparkContext().hadoopConfiguration().unset(Constants.PROPERTY_ENCRYPTION_KEY);
      sparkSession.sparkContext().hadoopConfiguration().unset(Constants.PROPERTY_ENCRYPTION_HASH);
    }
  }
}
