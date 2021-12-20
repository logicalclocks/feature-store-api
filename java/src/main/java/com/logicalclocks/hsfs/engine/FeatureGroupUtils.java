package com.logicalclocks.hsfs.engine;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.engine.hudi.HudiEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import lombok.SneakyThrows;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FeatureGroupUtils {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private KafkaApi kafkaApi = new KafkaApi();
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

  public <S> List<Feature> parseFeatureGroupSchema(S datasetGeneric)
      throws FeatureStoreException {
    if (engine().equals("spark")) {
      return SparkEngine.getInstance().parseFeatureGroupSchema(datasetGeneric);
    } else {
      throw new FeatureStoreException("This operation is only allowed from Spark engine.");
    }
  }

  public <S> S sanitizeFeatureNames(S datasetGeneric) throws FeatureStoreException {
    if (engine().equals("spark")) {
      return SparkEngine.getInstance().sanitizeFeatureNames(datasetGeneric);
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

  public Long getTimeStampFromDateString(String inputDate) throws FeatureStoreException, ParseException {

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

    config.put("kafka.bootstrap.servers",
        kafkaApi.getBrokerEndpoints(featureGroup.getFeatureStore()).stream().map(broker -> broker.replaceAll(
            "INTERNAL://", "")).collect(Collectors.joining(",")));
    config.put("kafka.security.protocol", "SSL");
    config.put("kafka.ssl.truststore.location", client.getTrustStorePath());
    config.put("kafka.ssl.truststore.password", client.getCertKey());
    config.put("kafka.ssl.keystore.location", client.getKeyStorePath());
    config.put("kafka.ssl.keystore.password", client.getCertKey());
    config.put("kafka.ssl.key.password", client.getCertKey());
    config.put("kafka.ssl.endpoint.identification.algorithm", "");
    return config;
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
    if (featureGroupBase instanceof FeatureGroup) {
      FeatureGroup featureGroup = (FeatureGroup) featureGroupBase;
      // operation is only valid for time travel enabled feature group
      if (featureGroup.getTimeTravelFormat() == TimeTravelFormat.NONE) {
        throw new FeatureStoreException("delete function is only valid for "
            + "time travel enabled feature group");
      }
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
    if (featureGroupBase instanceof FeatureGroup) {
      FeatureGroup featureGroup = (FeatureGroup) featureGroupBase;
      // operation is only valid for time travel enabled feature group
      if (featureGroup.getTimeTravelFormat() == TimeTravelFormat.NONE) {
        throw new FeatureStoreException("delete function is only valid for "
            + "time travel enabled feature group");
      }
    }

    if (engine().equals("Spark")) {
      HudiEngine hudiEngine = new HudiEngine();
      return hudiEngine.deleteRecord(SparkEngine.getInstance().getSparkSession(), featureGroupBase, genericDataset,
          writeOptions);
    } else {
      throw new FeatureStoreException("This operation is only allowed from Spark engine.");
    }
  }

  public String getAvroSchema(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
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

  private String engine() throws FeatureStoreException {
    if (checkIfClassExists("org.apache.spark.sql.Dataset")) {
      return "spark";
    } else {
      throw new FeatureStoreException("Unknown engine. Currently for java client only Spark engine is implemented.");
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
}
