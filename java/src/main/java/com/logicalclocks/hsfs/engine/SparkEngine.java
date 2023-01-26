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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.ExternalFeatureGroup;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.constructor.HudiFeatureGroupAlias;
import com.logicalclocks.hsfs.constructor.Query;
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
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.json.JSONObject;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.logicalclocks.hsfs.FeatureType.BIGINT;
import static com.logicalclocks.hsfs.FeatureType.DATE;
import static com.logicalclocks.hsfs.FeatureType.TIMESTAMP;
import static com.logicalclocks.hsfs.Split.SplitType.TIME_SERIES_SPLIT;
import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.avro.functions.to_avro;
import static org.apache.spark.sql.functions.array;
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

  // for testing
  public static void setInstance(SparkEngine sparkEngine) {
    INSTANCE = sparkEngine;
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
    sparkSession.conf().set("spark.sql.session.timeZone", "UTC");
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
    try {
      return sparkSession.sql(query);
    } catch (Exception e) {
      if (e.getMessage().contains("Permission denied")) {
        Pattern pattern = Pattern.compile("inode=\"/apps/hive/warehouse/(.*)?_featurestore\\.db\"");
        Matcher matcher = pattern.matcher(e.getMessage());
        if (matcher.find()) {
          String featureStore = matcher.group(1);
          throw new RuntimeException(String.format("Cannot access feature store '%s'. "
              + "It is possible to request access from data owners of '%s'.", featureStore, featureStore));
        }
      }
      throw e;
    }
  }

  public Dataset<Row> registerOnDemandTemporaryTable(ExternalFeatureGroup onDemandFeatureGroup, String alias)
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

  public static List<Dataset<Row>> splitLabels(Dataset<Row> dataset, List<String> labels) {
    List<Dataset<Row>> results = Lists.newArrayList();
    if (labels != null && !labels.isEmpty()) {
      Column[] labelsCol = labels.stream().map(label -> col(label).alias(label.toLowerCase())).toArray(Column[]::new);
      results.add(dataset.drop(labels.stream().toArray(String[]::new)));
      results.add(dataset.select(labelsCol));
    } else {
      results.add(dataset);
      results.add(null);
    }
    return results;
  }

  private Map<String, String> getOnDemandOptions(ExternalFeatureGroup externalFeatureGroup) {
    if (externalFeatureGroup.getOptions() == null) {
      return new HashMap<>();
    }

    return externalFeatureGroup.getOptions().stream()
        .collect(Collectors.toMap(OnDemandOptions::getName, OnDemandOptions::getValue));
  }

  public void registerHudiTemporaryTable(HudiFeatureGroupAlias hudiFeatureGroupAlias, Map<String, String> readOptions)
          throws FeatureStoreException {
    Map<String, String> hudiArgs = hudiEngine.setupHudiReadOpts(
        hudiFeatureGroupAlias.getLeftFeatureGroupStartTimestamp(),
        hudiFeatureGroupAlias.getLeftFeatureGroupEndTimestamp(),
        readOptions);

    sparkSession.read()
        .format(HudiEngine.HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .load(hudiFeatureGroupAlias.getFeatureGroup().getLocation())
        .createOrReplaceTempView(hudiFeatureGroupAlias.getAlias());

    hudiEngine.reconcileHudiSchema(sparkSession, hudiFeatureGroupAlias, hudiArgs);
  }

  /**
   * Setup Spark to write the data on the File System.
   *
   * @param trainingDataset Training Dataset metadata object
   * @param query Query Object
   * @param queryReadOptions Additional read options as key-value pairs, defaults to empty Map
   * @param writeOptions Additional write options as key-value pairs, defaults to empty Map
   * @param saveMode org.apache.spark.sql.saveMode: Append, Overwrite, ErrorIfExists, Ignore
   * @return Spark dataframe
   * @throws FeatureStoreException  FeatureStoreException
   * @throws IOException IOException
   */
  public Dataset<Row>[] write(TrainingDataset trainingDataset, Query query, Map<String, String> queryReadOptions,
                    Map<String, String> writeOptions, SaveMode saveMode) throws FeatureStoreException, IOException {
    setupConnectorHadoopConf(trainingDataset.getStorageConnector());

    if (trainingDataset.getSplits() == null || trainingDataset.getSplits().isEmpty()) {
      // Write a single dataset
      Dataset<Row> dataset = (Dataset<Row>) query.read();
      // The actual data will be stored in training_ds_version/training_ds the double directory is needed
      // for cases such as tfrecords in which we need to store also the schema
      // also in case of multiple splits, the single splits will be stored inside the training dataset dir
      String path = new Path(trainingDataset.getLocation(), trainingDataset.getName()).toString();
      if (trainingDataset.getCoalesce()) {
        dataset = dataset.coalesce(1);
      }
      writeSingle(dataset, trainingDataset.getDataFormat(), writeOptions, saveMode, path);
      return new Dataset[] {dataset};
    } else {
      Dataset<Row>[] datasetSplits = splitDataset(trainingDataset, query, queryReadOptions);
      if (trainingDataset.getCoalesce()) {
        for (int i = 0; i < datasetSplits.length; i++) {
          datasetSplits[i] = datasetSplits[i].coalesce(1);
        }
      }
      writeSplits(datasetSplits,
          trainingDataset.getDataFormat(), writeOptions, saveMode,
          trainingDataset.getLocation(), trainingDataset.getSplits());
      return datasetSplits;
    }
  }

  public Dataset<Row>[] splitDataset(TrainingDataset trainingDataset, Query query, Map<String, String> readOptions)
      throws FeatureStoreException, IOException {
    if (TIME_SERIES_SPLIT.equals(trainingDataset.getSplits().get(0).getSplitType())) {
      String eventTime = query.getLeftFeatureGroup().getEventTime();
      if (query.getLeftFeatures().stream().noneMatch(feature -> feature.getName().equals(eventTime))) {
        query.appendFeature(query.getLeftFeatureGroup().getFeature(eventTime));
        return timeSeriesSplit(trainingDataset, query, readOptions, true);
      } else {
        return timeSeriesSplit(trainingDataset, query, readOptions, false);
      }
    } else {
      return randomSplit(trainingDataset, query, readOptions);
    }
  }

  private Dataset<Row>[] timeSeriesSplit(TrainingDataset trainingDataset, Query query,
      Map<String, String> readOptions, Boolean dropEventTime)
      throws FeatureStoreException, IOException {
    Dataset<Row> dataset = (Dataset<Row>) query.read(false, readOptions);
    List<Split> splits = trainingDataset.getSplits();
    Dataset<Row>[] datasetSplits = new Dataset[splits.size()];
    dataset.persist();
    int i = 0;
    for (Split split : splits) {
      if (dataset.count() > 0) {
        String eventTime = query.getLeftFeatureGroup().getEventTime();
        String eventTimeType =
            query.getLeftFeatureGroup().getFeature(eventTime).getType();

        if (BIGINT.getType().equals(eventTimeType)) {
          String tmpEventTime = eventTime + "_hopsworks_tmp";
          sparkSession.sqlContext()
              .udf()
              .register("checkEpochUDF", (Long input) -> {
                if (Long.toString(input).length() > 10) {
                  input = input / 1000;
                  return input.longValue();
                } else {
                  return input;
                }
              }, DataTypes.LongType);
          dataset = dataset.withColumn(tmpEventTime,functions.callUDF(
              "checkEpochUDF", dataset.col(eventTime)));

          // event time in second. `getTime()` return in millisecond.
          datasetSplits[i] = dataset.filter(
              String.format(
                  "%d/1000 <= `%s` and `%s` < %d/1000",
                  split.getStartTime().getTime(),
                  tmpEventTime,
                  tmpEventTime,
                  split.getEndTime().getTime()
              )
          ).drop(tmpEventTime);
        } else if (DATE.getType().equals(eventTimeType) || TIMESTAMP.getType().equals(eventTimeType)) {
          // unix_timestamp return in second. `getTime()` return in millisecond.
          datasetSplits[i] = dataset.filter(
              String.format(
                  "%d/1000 <= unix_timestamp(`%s`) and unix_timestamp(`%s`) < %d/1000",
                  split.getStartTime().getTime(),
                  eventTime,
                  eventTime,
                  split.getEndTime().getTime()
              )
          );
        } else {
          throw new FeatureStoreException("Invalid event time type");
        }
      } else {
        datasetSplits[i] = dataset;
      }
      i++;
    }
    return datasetSplits;
  }

  private Dataset<Row>[] randomSplit(TrainingDataset trainingDataset, Query query, Map<String, String> readOptions)
      throws FeatureStoreException, IOException {
    Dataset<Row> dataset = (Dataset<Row>) query.read(false, readOptions);

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
   * @param datasets Spark DataFrame or RDD.
   * @param dataFormat DataFormat: CSV, TSV, PARQUET, AVRO, IMAGE, ORC, TFRECORDS, TFRECORD
   * @param writeOptions Additional write options as key-value pairs, defaults to empty Map
   * @param saveMode saveMode org.apache.spark.sql.saveMode: Append, Overwrite, ErrorIfExists, Ignore
   * @param basePath Base path to training dataset file
   * @param splits list of training dataset Split metadata objects
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
   * @param dataset Spark DataFrame or RDD.
   * @param dataFormat DataFormat: CSV, TSV, PARQUET, AVRO, IMAGE, ORC, TFRECORDS, TFRECORD
   * @param writeOptions Additional write options as key-value pairs, defaults to empty Map
   * @param saveMode saveMode org.apache.spark.sql.saveMode: Append, Overwrite, ErrorIfExists, Ignore
   * @param path full path to training dataset file
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

  public <S> void writeOnlineDataframe(FeatureGroupBase featureGroupBase, S dataset, String onlineTopicName,
                                         Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    byte[] version = String.valueOf(featureGroupBase.getSubject().getVersion()).getBytes(StandardCharsets.UTF_8);

    onlineFeatureGroupToAvro(featureGroupBase, encodeComplexFeatures(featureGroupBase, (Dataset<Row>) dataset))
        .withColumn("headers", array(
            struct(
                lit("version").as("key"),
                lit(version).as("value")
            )
        ))
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
    byte[] version = String.valueOf(featureGroupBase.getSubject().getVersion()).getBytes(StandardCharsets.UTF_8);

    Dataset<Row> dataset = (Dataset<Row>) datasetGeneric;
    DataStreamWriter<Row> writer =
        onlineFeatureGroupToAvro(featureGroupBase, encodeComplexFeatures(featureGroupBase, dataset))
        .withColumn("headers", array(
            struct(
                lit("version").as("key"),
                lit(version).as("value")
            )
        ))
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
   * @param featureGroupBase FeatureGroupBase Feature Group base metadata object
   * @param dataset Spark DataFrame or RDD.
   * @return Spark DataFrame.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
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
   * @param featureGroupBase FeatureGroupBase Feature Group base metadata object
   * @param dataset  Spark DataFrame or RDD.
   * @return Spark DataFrame.
   * @throws FeatureStoreException FeatureStoreException
   * @throws IOException IOException
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

  public <S>  void writeEmptyDataframe(FeatureGroupBase featureGroup)
          throws IOException, FeatureStoreException, ParseException {
    String fgTableName = utils.getTableName(featureGroup);
    Dataset emptyDf = sparkSession.table(fgTableName).limit(0);
    writeOfflineDataframe(featureGroup, emptyDf, HudiOperationType.UPSERT, new HashMap<>(), null);
  }

  public <S>  void writeOfflineDataframe(StreamFeatureGroup streamFeatureGroup, S genericDataset,
      HudiOperationType operation, Map<String, String> writeOptions, Integer validationId)
      throws IOException, FeatureStoreException, ParseException {

    Dataset<Row> dataset = (Dataset<Row>) genericDataset;
    hudiEngine.saveHudiFeatureGroup(sparkSession, streamFeatureGroup, dataset, operation, writeOptions, validationId);
  }

  public void writeOfflineDataframe(FeatureGroupBase featureGroup, Dataset<Row> dataset,
                                    HudiOperationType operation, Map<String, String> writeOptions, Integer validationId)
      throws IOException, FeatureStoreException, ParseException {

    if (featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI) {
      hudiEngine.saveHudiFeatureGroup(sparkSession, featureGroup, dataset, operation, writeOptions, validationId);
    } else {
      writeSparkDataset(featureGroup, dataset, writeOptions);
    }
  }

  private void writeSparkDataset(FeatureGroupBase featureGroup, Dataset<Row> dataset,
                                 Map<String, String> writeOptions) {
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

  public void setupConnectorHadoopConf(StorageConnector storageConnector)
          throws FeatureStoreException, IOException {
    if (storageConnector == null) {
      return;
    }

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

  public void streamToHudiTable(StreamFeatureGroup streamFeatureGroup, Map<String, String> writeOptions)
      throws Exception {
    writeOptions = utils.getKafkaConfig(streamFeatureGroup, writeOptions);
    hudiEngine.streamToHoodieTable(sparkSession, streamFeatureGroup, writeOptions);
  }

  public <S> List<Feature> parseFeatureGroupSchema(S datasetGeneric,
      TimeTravelFormat timeTravelFormat) throws FeatureStoreException {
    List<Feature> features = new ArrayList<>();
    Dataset<Row> dataset = (Dataset<Row>) datasetGeneric;
    Boolean usingHudi = timeTravelFormat == TimeTravelFormat.HUDI;
    for (StructField structField : dataset.schema().fields()) {
      String featureType = "";
      if (!usingHudi) {
        featureType = structField.dataType().catalogString();
      } else if (structField.dataType() instanceof ByteType) {
        featureType = "int";
      } else if (structField.dataType() instanceof ShortType) {
        featureType = "int";
      } else if (structField.dataType() instanceof BooleanType
          || structField.dataType() instanceof IntegerType
          || structField.dataType() instanceof LongType
          || structField.dataType() instanceof FloatType
          || structField.dataType() instanceof DoubleType
          || structField.dataType() instanceof DecimalType
          || structField.dataType() instanceof TimestampType
          || structField.dataType() instanceof DateType
          || structField.dataType() instanceof StringType
          || structField.dataType() instanceof ArrayType
          || structField.dataType() instanceof StructType
          || structField.dataType() instanceof BinaryType) {
        featureType = structField.dataType().catalogString();
      } else {
        throw new FeatureStoreException("Feature '" + structField.name().toLowerCase() + "': "
            + "spark type " + structField.dataType().catalogString() + " not supported.");
      }

      Feature f = new Feature(structField.name().toLowerCase(), featureType, false, false);
      if (structField.metadata().contains("description")) {
        f.setDescription(structField.metadata().getString("description"));
      }

      features.add(f);
    }

    return features;
  }

  public Dataset<Row> castColumnType(Dataset<Row> dataset, List<TrainingDatasetFeature> features)
      throws FeatureStoreException {
    for (TrainingDatasetFeature feature: features) {
      dataset = dataset.withColumn(
          feature.getName(),
          dataset.col(feature.getName()).cast(convertColumnType(feature.getType()))
      );
    }
    return dataset;
  }

  private static Pattern arrayPattern = Pattern.compile("^array<(.*)>$");

  private DataType convertColumnType(String type) throws FeatureStoreException {
    Matcher m = arrayPattern.matcher(type);
    if (m.matches()) {
      return DataTypes.createArrayType(convertColumnType(m.group(1)));
    } else if (type.contains("struct<label:string,index:int>")) {
      StructField label = new StructField("label", DataTypes.StringType, true, Metadata.empty());
      StructField index = new StructField("index", DataTypes.IntegerType, true, Metadata.empty());
      return DataTypes.createStructType(new StructField[]{label, index});
    } else {
      return convertBasicType(type);
    }
  }

  private DataType convertBasicType(String type) throws FeatureStoreException {
    Map<String, DataType> pyarrowToSparkType = Maps.newHashMap();
    pyarrowToSparkType.put("string", DataTypes.StringType);
    pyarrowToSparkType.put("bigint", DataTypes.LongType);
    pyarrowToSparkType.put("int", DataTypes.IntegerType);
    pyarrowToSparkType.put("smallint", DataTypes.ShortType);
    pyarrowToSparkType.put("tinyint", DataTypes.ByteType);
    pyarrowToSparkType.put("float", DataTypes.FloatType);
    pyarrowToSparkType.put("double", DataTypes.DoubleType);
    pyarrowToSparkType.put("timestamp", DataTypes.TimestampType);
    pyarrowToSparkType.put("boolean", DataTypes.BooleanType);
    pyarrowToSparkType.put("date", DataTypes.DateType);
    pyarrowToSparkType.put("binary", DataTypes.BinaryType);
    pyarrowToSparkType.put("decimal", DataTypes.createDecimalType());
    if (pyarrowToSparkType.containsKey(type)) {
      return pyarrowToSparkType.get(type);
    } else {
      throw new FeatureStoreException(String.format("Pyarrow type %s cannot be converted to a spark type.", type));
    }
  }

  public <S> S convertToDefaultDataframe(S datasetGeneric) {
    Dataset<Row> dataset = (Dataset<Row>) datasetGeneric;
    Dataset<Row> sanitizedNamesDataset = dataset.select(Arrays.asList(dataset.columns()).stream().map(f ->
            col(f).alias(f.toLowerCase())).toArray(Column[]::new));

    // for streaming dataframes this will be handled in DeltaStreamerTransformer.java class
    if (!dataset.isStreaming()) {
      StructType schema = sanitizedNamesDataset.schema();
      StructType nullableSchema = new StructType(JavaConverters.asJavaCollection(schema.toSeq()).stream().map(f ->
          new StructField(f.name(), f.dataType(), true, f.metadata())
      ).toArray(StructField[]::new));
      Dataset<Row> nullableDataset = sanitizedNamesDataset.sparkSession()
          .createDataFrame(sanitizedNamesDataset.rdd(), nullableSchema);
      return (S) nullableDataset;
    } else {
      return (S) sanitizedNamesDataset;
    }
  }

  public String addFile(String filePath) {
    // this is used for unit testing
    if (!filePath.startsWith("file://")) {
      filePath = "hdfs://" + filePath;
    }
    sparkSession.sparkContext().addFile(filePath);
    return SparkFiles.get((new Path(filePath)).getName());
  }

  public Dataset<Row> readStream(StorageConnector storageConnector, String dataFormat, String messageFormat,
                                 String schema, Map<String, String> options, boolean includeMetadata)
      throws FeatureStoreException, IOException {
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

  private void setupGcsConnectorHadoopConf(StorageConnector.GcsConnector storageConnector) throws IOException {
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
    String fileContent = Files.lines(Paths.get(localPath), StandardCharsets.UTF_8)
        .collect(Collectors.joining("\n"));
    JSONObject jsonObject = new JSONObject(fileContent);

    // set the account properties instead of key file path
    sparkSession.sparkContext().hadoopConfiguration().set(
        Constants.PROPERTY_GCS_ACCOUNT_EMAIL, jsonObject.getString("client_email")
    );
    sparkSession.sparkContext().hadoopConfiguration().set(
        Constants.PROPERTY_GCS_ACCOUNT_KEY_ID, jsonObject.getString("private_key_id")
    );
    sparkSession.sparkContext().hadoopConfiguration().set(
        Constants.PROPERTY_GCS_ACCOUNT_KEY, jsonObject.getString("private_key")
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

  public <S> S createEmptyDataFrame(S datasetGeneric) {
    Dataset<Row> dataset = (Dataset<Row>) datasetGeneric;
    List<Row> rows = new ArrayList<Row>();
    return (S) sparkSession.sqlContext().createDataFrame(rows, dataset.schema());
  }
}
