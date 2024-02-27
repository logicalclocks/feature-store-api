/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.metadata.Subject;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;

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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FeatureGroupUtils {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private KafkaApi kafkaApi = new KafkaApi();
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");

  // TODO(Fabio): this should be moved in the backend
  public String getTableName(FeatureGroupBase offlineFeatureGroup) {
    return offlineFeatureGroup.getFeatureStore().getName() + "." + getFgName(offlineFeatureGroup);
  }

  public Seq<String> getPartitionColumns(FeatureGroupBase offlineFeatureGroup) {
    List<Feature> features = offlineFeatureGroup.getFeatures();
    List<String> partitionCols = features.stream()
        .filter(Feature::getPartition)
        .map(Feature::getName)
        .collect(Collectors.toList());

    return JavaConverters.asScalaIteratorConverter(partitionCols.iterator()).asScala().toSeq();
  }

  public Seq<String> getPrimaryColumns(FeatureGroupBase offlineFeatureGroup) {
    List<Feature> features = offlineFeatureGroup.getFeatures();
    List<String> primaryCols = features.stream()
        .filter(Feature::getPrimary)
        .map(Feature::getName)
        .collect(Collectors.toList());

    return JavaConverters.asScalaIteratorConverter(primaryCols.iterator()).asScala().toSeq();
  }

  public String getFgName(FeatureGroupBase featureGroup) {
    return featureGroup.getName() + "_" + featureGroup.getVersion();
  }

  public String getHiveServerConnection(FeatureGroupBase featureGroup, String connectionString)
      throws IOException, FeatureStoreException {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("sslTrustStore", HopsworksClient.getInstance().getHopsworksHttpClient().getTrustStorePath());
    credentials.put("trustStorePassword", HopsworksClient.getInstance().getHopsworksHttpClient().getCertKey());
    credentials.put("sslKeyStore", HopsworksClient.getInstance().getHopsworksHttpClient().getKeyStorePath());
    credentials.put("keyStorePassword", HopsworksClient.getInstance().getHopsworksHttpClient().getCertKey());

    return connectionString
        + credentials.entrySet().stream().map(cred -> cred.getKey() + "=" + cred.getValue())
        .collect(Collectors.joining(";"));
  }

  public static Date getDateFromDateString(String inputDate) throws FeatureStoreException, ParseException {
    if (inputDate != null) {
      return new Date(getTimeStampFromDateString(inputDate));
    } else {
      return null;
    }
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

  public Map<Long, Map<String, String>>  getCommitDetails(FeatureGroupBase featureGroup, String wallclockTime,
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

  public List<String> getComplexFeatures(List<Feature> features) {
    return features.stream().filter(Feature::isComplex).map(Feature::getName).collect(Collectors.toList());
  }

  public String getFeatureAvroSchema(String featureName, Schema schema) throws FeatureStoreException, IOException {
    Schema.Field complexField = schema.getFields().stream().filter(field ->
        field.name().equalsIgnoreCase(featureName)).findFirst().orElseThrow(() ->
        new FeatureStoreException(
            "Complex feature `" + featureName + "` not found in AVRO schema of online feature group."));
    return complexField.schema().toString(false);
  }

  public Schema getDeserializedEncodedAvroSchema(Schema schema, List<String> complexFeatures)
      throws FeatureStoreException, IOException {
    List<Schema.Field> fields = schema.getFields().stream()
        .map(field -> complexFeatures.contains(field.name())
          ? new Schema.Field(field.name(), SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion(),
          null, null)
          : new Schema.Field(field.name(), field.schema(), null, null))
        .collect(Collectors.toList());
    return Schema.createRecord(schema.getName(), null, schema.getNamespace(), schema.isError(), fields);
  }

  public String getEncodedAvroSchema(Schema schema, List<String> complexFeatures)
      throws FeatureStoreException, IOException {
    Schema deserializedEncodedAvroSchema = getDeserializedEncodedAvroSchema(schema, complexFeatures);
    return deserializedEncodedAvroSchema.toString(false);
  }

  public Schema getDeserializedAvroSchema(String avroSchema) throws FeatureStoreException, IOException {
    try {
      return new Schema.Parser().parse(avroSchema);
    } catch (SchemaParseException e) {
      throw new FeatureStoreException("Failed to deserialize online feature group schema" + avroSchema + ".");
    }
  }

  public void verifyAttributeKeyNames(FeatureGroupBase featureGroup, List<String> partitionKeyNames,
                                      String precombineKeyName) throws FeatureStoreException {
    List<Feature> features = featureGroup.getFeatures();
    List<String> featureNames = features.stream().map(Feature::getName).collect(Collectors.toList());
    if (featureGroup.getPrimaryKeys() != null && !featureGroup.getPrimaryKeys().isEmpty()) {
      checkListdiff(featureGroup.getPrimaryKeys(), featureNames, "primary");
    }

    if (partitionKeyNames != null && !partitionKeyNames.isEmpty()) {
      checkListdiff(partitionKeyNames, featureNames, "partition");
    }

    if (precombineKeyName != null && !featureNames.contains(precombineKeyName)) {
      throw new FeatureStoreException("Provided Hudi precombine key " + precombineKeyName
          + " doesn't exist in feature dataframe");
    }

    if (featureGroup.getEventTime() != null && !featureNames.contains(featureGroup.getEventTime())) {
      throw new FeatureStoreException("Provided eventTime feature name " + featureGroup.getEventTime()
          + " doesn't exist in feature dataframe");
    }
  }

  private void checkListdiff(List<String> primaryPartitionKeyNames, List<String> featureNames, String attributeName)
      throws FeatureStoreException {
    List<String> differences = primaryPartitionKeyNames.stream()
        .filter(element -> !featureNames.contains(element))
        .collect(Collectors.toList());

    if (!differences.isEmpty()) {
      throw new FeatureStoreException("Provided " + attributeName + " key(s) " + String.join(", ",
          differences) +  " doesn't exist in feature dataframe");
    }
  }

  public Subject getSubject(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    return kafkaApi.getSubject(featureGroup.getFeatureStore(), getFgName(featureGroup));
  }

  public String getDatasetType(String path) {
    if (Pattern.compile("^(?:hdfs://|)/apps/hive/warehouse/*").matcher(path).find()) {
      return "HIVEDB";
    }
    return "DATASET";
  }
}
