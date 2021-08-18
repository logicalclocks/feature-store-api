/*
 * Copyright (c) 2022 Logical Clocks AB
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

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.TimeTravelFormat;

import com.logicalclocks.hsfs.engine.flink.FlinkEngine;
import com.logicalclocks.hsfs.engine.hudi.HudiEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import lombok.SneakyThrows;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FeatureGroupUtils {

  private final FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private final StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private final KafkaApi kafkaApi = new KafkaApi();
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  public final String engineType = engineType();

  public FeatureGroupUtils() {
  }

  public <S> List<Feature> parseFeatureGroupSchema(S datasetGeneric, TimeTravelFormat timeTravelFormat)
      throws FeatureStoreException {
    if (engineType.equals("spark")) {
      return SparkEngine.getInstance().parseFeatureGroupSchema(datasetGeneric, timeTravelFormat);
    } else if (engineType.equals("flink")) {
      return FlinkEngine.getInstance().parseFeatureGroupSchema(datasetGeneric);
    } else {
      throw new FeatureStoreException("This operation is only allowed from Spark engine.");
    }
  }

  public <S> S sanitizeFeatureNames(S datasetGeneric) throws FeatureStoreException {
    if (engineType.equals("spark")) {
      return SparkEngine.getInstance().sanitizeFeatureNames(datasetGeneric);
    } else if (engineType.equals("flink")) {
      return FlinkEngine.getInstance().sanitizeFeatureNames(datasetGeneric);
    } else {
      throw new FeatureStoreException("This operation is only allowed from Spark engine.");
    }
  }

  // TODO(Fabio): this should be moved in the backend
  public String getTableName(FeatureGroup offlineFeatureGroup) {
    return offlineFeatureGroup.getFeatureStore().getName() + "."
        + offlineFeatureGroup.getName() + "_" + offlineFeatureGroup.getVersion();
  }

  public String getOnlineTableName(FeatureGroup offlineFeatureGroup) {
    return offlineFeatureGroup.getName() + "_" + offlineFeatureGroup.getVersion();
  }

  public Seq<String> getPartitionColumns(FeatureGroupBase offlineFeatureGroup) {
    List<String> partitionCols = offlineFeatureGroup.getFeatures().stream()
        .filter(Feature::getPartition)
        .map(Feature::getName)
        .collect(Collectors.toList());

    return JavaConverters.asScalaIteratorConverter(partitionCols.iterator()).asScala().toSeq();
  }

  public Seq<String> getPrimaryColumns(FeatureGroupBase offlineFeatureGroup) {
    List<String> primaryCols = offlineFeatureGroup.getFeatures().stream()
        .filter(Feature::getPrimary)
        .map(Feature::getName)
        .collect(Collectors.toList());

    return JavaConverters.asScalaIteratorConverter(primaryCols.iterator()).asScala().toSeq();
  }

  public String getFgName(FeatureGroupBase featureGroup) {
    return featureGroup.getName() + "_" + featureGroup.getVersion();
  }

  public String getHiveServerConnection(FeatureGroupBase featureGroup) throws IOException, FeatureStoreException {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("sslTrustStore", HopsworksClient.getInstance().getHopsworksHttpClient().getTrustStorePath());
    credentials.put("trustStorePassword", HopsworksClient.getInstance().getHopsworksHttpClient().getCertKey());
    credentials.put("sslKeyStore", HopsworksClient.getInstance().getHopsworksHttpClient().getKeyStorePath());
    credentials.put("keyStorePassword", HopsworksClient.getInstance().getHopsworksHttpClient().getCertKey());

    StorageConnector.JdbcConnector storageConnector =
        (StorageConnector.JdbcConnector) storageConnectorApi.getByName(featureGroup.getFeatureStore(),
            featureGroup.getFeatureStore().getName());

    return storageConnector.getConnectionString()
        + credentials.entrySet().stream().map(cred -> cred.getKey() + "=" + cred.getValue())
        .collect(Collectors.joining(";"));
  }

  public static Long getTimeStampFromDateString(String inputDate) throws FeatureStoreException, ParseException {

    HashMap<Pattern, String> dateFormatPatterns = new HashMap<Pattern, String>() {{
        put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})$"), "yyyyMMdd");
        put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})$"), "yyyyMMddHH");
        put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$"), "yyyyMMddHHmm");
        put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$"), "yyyyMMddHHmmss");
        put(Pattern.compile("^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{3})$"),
            "yyyyMMddHHmmssSSS");
      }};

    String tempDate = inputDate.replace("/", "")
        .replace("-", "").replace(" ", "")
        .replace(":","");
    String dateFormatPattern = null;

    for (Pattern pattern : dateFormatPatterns.keySet()) {
      if (pattern.matcher(tempDate).matches()) {
        dateFormatPattern = dateFormatPatterns.get(pattern);
        break;
      }
    }

    if (dateFormatPattern == null) {
      throw new FeatureStoreException("Unable to identify format of the provided date value : " + inputDate);
    }

    SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);
    Long commitTimeStamp = dateFormat.parse(tempDate).getTime();;

    return commitTimeStamp;
  }

  @SneakyThrows
  public String timeStampToHudiFormat(Long commitedOnTimeStamp) {
    Date commitedOnDate = new Timestamp(commitedOnTimeStamp);
    return dateFormat.format(commitedOnDate);
  }


  public String constructCheckpointPath(FeatureGroup featureGroup, String queryName, String queryPrefix)
      throws FeatureStoreException {
    if (Strings.isNullOrEmpty(queryName)) {
      queryName = queryPrefix + featureGroup.getOnlineTopicName() + "_" + LocalDateTime.now().format(
          DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }
    return "/Projects/" + HopsworksClient.getInstance().getProject().getProjectName()
        + "/Resources/" + queryName + "-checkpoint";
  }

  public Map<String, String> getKafkaConfig(FeatureGroupBase featureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    Map<String, String> config = new HashMap<>();
    if (writeOptions != null) {
      config.putAll(writeOptions);
    }
    HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();

    // spark and flink require different key values for kafka config
    config.put(engineType.equals("spark") ? "kafka.bootstrap.servers" : "bootstrap.servers",
        kafkaApi.getBrokerEndpoints(featureGroup.getFeatureStore()).stream().map(broker -> broker.replaceAll(
            "INTERNAL://", "")).collect(Collectors.joining(",")));
    config.put(engineType.equals("spark") ? "kafka.security.protocol" : "security.protocol", "SSL");
    config.put(engineType.equals("spark") ? "kafka.ssl.truststore.location" :
        "security.ssl.truststore", client.getTrustStorePath());
    config.put(engineType.equals("spark") ? "kafka.ssl.truststore.password" :
        "security.ssl.truststore-password", client.getCertKey());
    config.put(engineType.equals("spark") ? "kafka.ssl.keystore.location" :
        "security.ssl.keystore", client.getKeyStorePath());
    config.put(engineType.equals("spark") ? "kafka.ssl.keystore.password" :
        "security.ssl.keystore-password", client.getCertKey());
    config.put(engineType.equals("spark") ? "kafka.ssl.key.password" :
        "security.ssl.key-password", client.getCertKey());
    config.put(engineType.equals("spark") ? "kafka.ssl.endpoint.identification.algorithm" :
        "security.ssl.algorithms", "");
    return config;
  }

  public Properties getKafkaProperties(FeatureGroupBase featureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    Map<String, String> kafkaConfig = getKafkaConfig(featureGroup,  writeOptions);
    Properties properties = new Properties();
    properties.put("group.id",  featureGroup.getName() + "-" + featureGroup.getVersion());
    properties.putAll(kafkaConfig);
    return properties;
  }

  private Map<Long, Map<String, String>>  getCommitDetails(FeatureGroupBase featureGroup, String wallclockTime,
                                                           Integer limit)
      throws FeatureStoreException, IOException, ParseException {

    Long wallclockTimestamp =  wallclockTime != null ? getTimeStampFromDateString(wallclockTime) : null;
    List<FeatureGroupCommit> featureGroupCommits =
        featureGroupApi.getCommitDetails(featureGroup, wallclockTimestamp, limit);
    if (featureGroupCommits == null) {
      throw new FeatureStoreException("There are no commit details available for this Feature group");
    }
    Map<Long, Map<String, String>> commitDetails = new HashMap<>();
    for (FeatureGroupCommit featureGroupCommit : featureGroupCommits) {
      commitDetails.put(featureGroupCommit.getCommitID(), new HashMap<String, String>() {{
            put("committedOn", timeStampToHudiFormat(featureGroupCommit.getCommitID()));
            put("rowsUpdated", featureGroupCommit.getRowsUpdated() != null
                ? featureGroupCommit.getRowsUpdated().toString() : "0");
            put("rowsInserted", featureGroupCommit.getRowsInserted() != null
                ? featureGroupCommit.getRowsInserted().toString() : "0");
            put("rowsDeleted", featureGroupCommit.getRowsDeleted() != null
                ? featureGroupCommit.getRowsDeleted().toString() : "0");
          }}
      );
    }
    return commitDetails;
  }

  public Map<Long, Map<String, String>> commitDetails(FeatureGroupBase featureGroupBase, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    // operation is only valid for time travel enabled feature group
    if (!((featureGroupBase instanceof FeatureGroup && featureGroupBase.getTimeTravelFormat() == TimeTravelFormat.NONE)
        || featureGroupBase instanceof StreamFeatureGroup)) {
      // operation is only valid for time travel enabled feature group
      throw new FeatureStoreException("delete function is only valid for "
            + "time travel enabled feature group");
    }
    return getCommitDetails(featureGroupBase, null, limit);
  }

  public Map<Long, Map<String, String>> commitDetailsByWallclockTime(FeatureGroupBase featureGroup,
                                                                     String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return getCommitDetails(featureGroup, wallclockTime, limit);
  }

  public <S> FeatureGroupCommit commitDelete(FeatureGroupBase featureGroupBase, S genericDataset,
                                         Map<String, String> writeOptions)
      throws IOException, FeatureStoreException, ParseException {
    if (!((featureGroupBase instanceof FeatureGroup && featureGroupBase.getTimeTravelFormat() == TimeTravelFormat.NONE)
        || featureGroupBase instanceof StreamFeatureGroup)) {
      // operation is only valid for time travel enabled feature group
      throw new FeatureStoreException("delete function is only valid for "
            + "time travel enabled feature group");
    }

    if (engineType.equals("spark")) {
      HudiEngine hudiEngine = new HudiEngine();
      return hudiEngine.deleteRecord(SparkEngine.getInstance().getSparkSession(), featureGroupBase, genericDataset,
          writeOptions);
    } else {
      throw new FeatureStoreException("This operation is only allowed from Spark engine.");
    }
  }

  public String getAvroSchema(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    return kafkaApi.getTopicSubject(featureGroup.getFeatureStore(), featureGroup.getOnlineTopicName()).getSchema();
  }

  private boolean checkIfClassExists(String className) {
    try  {
      Class.forName(className, true, this.getClass().getClassLoader());
      return true;
    }  catch (ClassNotFoundException e) {
      return false;
    }
  }

  public String engineType() {
    if (checkIfClassExists("org.apache.spark.sql.Dataset")) {
      return "spark";
    } else if (checkIfClassExists("org.apache.flink.streaming.api.datastream.DataStream")) {
      return "flink";
    } else {
      return null;
    }
  }

  public String checkpointDirPath(String queryName, String onlineTopicName) throws FeatureStoreException {
    if (Strings.isNullOrEmpty(queryName)) {
      queryName = "insert_stream_" + onlineTopicName + "_" + LocalDateTime.now().format(
          DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }
    return "/Projects/" + HopsworksClient.getInstance().getProject().getProjectName()
        + "/Resources/" + queryName + "-checkpoint";

  }

  public List<String> getComplexFeatures(List<Feature> features) {
    return features.stream().filter(Feature::isComplex).map(Feature::getName).collect(Collectors.toList());
  }

  public String getFeatureAvroSchema(String featureName, Schema schema) throws FeatureStoreException, IOException {
    Schema.Field complexField = schema.getFields().stream().filter(field ->
        field.name().equalsIgnoreCase(featureName)).findFirst().orElseThrow(() ->
        new FeatureStoreException(
            "Complex feature `" + featureName + "` not found in AVRO schema of online feature group."));
    return complexField.schema().toString(true);
  }

  public String getEncodedAvroSchema(Schema schema, List<String> complexFeatures)
      throws FeatureStoreException, IOException {
    List<Schema.Field> fields = schema.getFields().stream()
        .map(field -> complexFeatures.contains(field.name())
            ? new Schema.Field(field.name(), SchemaBuilder.builder().nullable().bytesType(), null, null)
            : new Schema.Field(field.name(), field.schema(), null, null))
        .collect(Collectors.toList());
    return Schema.createRecord(schema.getName(), null, schema.getNamespace(), schema.isError(), fields).toString(true);
  }

  public Schema getDeserializedAvroSchema(String avroSchema) throws FeatureStoreException, IOException {
    try {
      return new Schema.Parser().parse(avroSchema);
    } catch (SchemaParseException e) {
      throw new FeatureStoreException("Failed to deserialize online feature group schema" + avroSchema + ".");
    }
  }

  public static Date getDateFromDateString(String inputDate) throws FeatureStoreException, ParseException {
    if (inputDate != null) {
      return new Date(getTimeStampFromDateString(inputDate));
    } else {
      return null;
    }
  }
}
