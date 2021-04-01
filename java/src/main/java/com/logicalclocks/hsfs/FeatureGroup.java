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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.hsfs.engine.DataValidationEngine;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.metadata.Expectation;
import com.logicalclocks.hsfs.metadata.ExpectationsApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupValidation;
import com.logicalclocks.hsfs.metadata.validation.ValidationType;
import com.logicalclocks.hsfs.metadata.Statistics;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.Schema;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  private Boolean onlineEnabled;

  @Getter
  @Setter
  private String type = "cachedFeaturegroupDTO";

  @Getter
  @Setter
  private TimeTravelFormat timeTravelFormat = TimeTravelFormat.HUDI;

  @Getter
  @Setter
  protected String location;

  @Getter
  @Setter
  @JsonProperty("validationType")
  private ValidationType validationType = ValidationType.NONE;

  @Getter
  @Setter
  private List<String> statisticColumns;

  @Getter @Setter
  private List<String> expectationsNames;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> primaryKeys;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  @JsonIgnore
  // This is only used in the client. In the server they are aggregated in the `features` field
  private String hudiPrecombineKey;

  @JsonIgnore
  private String avroSchema;

  @Getter
  @Setter
  private String onlineTopicName;

  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final ExpectationsApi expectationsApi = new ExpectationsApi(EntityEndpointType.FEATURE_GROUP);

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Builder
  public FeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                      List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                      boolean onlineEnabled, TimeTravelFormat timeTravelFormat, List<Feature> features,
                      StatisticsConfig statisticsConfig,  ValidationType validationType,
                      scala.collection.Seq<Expectation> expectations, String onlineTopicName) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
        ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
        ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = timeTravelFormat == TimeTravelFormat.HUDI && hudiPrecombineKey != null
        ? hudiPrecombineKey.toLowerCase() : null;
    this.onlineEnabled = onlineEnabled;
    this.timeTravelFormat = timeTravelFormat != null ? timeTravelFormat : TimeTravelFormat.HUDI;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.validationType = validationType != null ? validationType : ValidationType.NONE;
    if (expectations != null && !expectations.isEmpty()) {
      this.expectationsNames = new ArrayList<>();
      ((List<Expectation>) JavaConverters.seqAsJavaListConverter(expectations).asJava())
        .forEach(expectation -> this.expectationsNames.add(expectation.getName()));
    }
    this.onlineTopicName = onlineTopicName;
  }

  public FeatureGroup() {
  }

  public void updateValidationType(ValidationType validationType) throws FeatureStoreException, IOException {
    this.validationType = validationType;
    featureGroupEngine.updateValidationType(this);
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return selectAll().read(online);
  }

  public Dataset<Row> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return read(false, readOptions);
  }

  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return selectAll().read(online, readOptions);
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   */
  public Dataset<Row> read(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime).read(false, null);
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime
   * @param readOptions
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   */
  public Dataset<Row> read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime).read(false, readOptions);
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   */
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, null);
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return DataFrame.
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   */
  public Dataset<Row> readChanges(String wallclockStartTime, String wallclockEndTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime).read(false, readOptions);
  }


  public void show(int numRows) throws FeatureStoreException, IOException {
    show(numRows, false);
  }

  public void show(int numRows, boolean online) throws FeatureStoreException, IOException {
    read(online).show(numRows);
  }

  public void save(Dataset<Row> featureData) throws FeatureStoreException, IOException, ParseException {
    save(featureData, null);
  }

  public void save(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.save(this, featureData, primaryKeys, partitionKeys, hudiPrecombineKey,
        writeOptions);
    if (statisticsConfig.getEnabled()) {
      statisticsEngine.computeStatistics(this, featureData, null);
    }
  }

  public void insert(Dataset<Row> featureData) throws IOException, FeatureStoreException, ParseException {
    insert(featureData, null, false);
  }

  public void insert(Dataset<Row> featureData,  Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, false, null, writeOptions);
  }

  public void insert(Dataset<Row> featureData, Storage storage)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, storage, false, null, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, null, overwrite);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    insert(featureData, storage, overwrite, null, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, overwrite, null, writeOptions);
  }

  /**
   * Commit insert or upsert to time travel enabled Feature group.
   *
   * @param featureData dataframe to be committed.
   * @param operation   commit operation type, INSERT or UPSERT.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void insert(Dataset<Row> featureData, HudiOperationType operation)
      throws FeatureStoreException, IOException, ParseException {
    insert(featureData, null, false, operation, null);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite, HudiOperationType operation,
                     Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {

    // operation is only valid for time travel enabled feature group
    if (operation != null && this.timeTravelFormat == TimeTravelFormat.NONE) {
      throw new IllegalArgumentException("operation argument is valid only for time travel enable feature groups");
    }

    if (operation == null && this.timeTravelFormat == TimeTravelFormat.HUDI) {
      if (overwrite) {
        operation = HudiOperationType.BULK_INSERT;
      } else {
        operation = HudiOperationType.UPSERT;
      }
    }

    featureGroupEngine.insert(this, featureData, storage, operation,
        overwrite ? SaveMode.Overwrite : SaveMode.Append, writeOptions);

    computeStatistics();
  }

  public StreamingQuery insertStream(Dataset<Row> featureData)
      throws StreamingQueryException, IOException, FeatureStoreException {
    return insertStream(featureData, null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName)
      throws StreamingQueryException, IOException, FeatureStoreException {
    return insertStream(featureData, queryName, "append");
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode)
      throws StreamingQueryException, IOException, FeatureStoreException {
    return insertStream(featureData, queryName, outputMode, false, null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
      boolean awaitTermination, Long timeout) throws StreamingQueryException, IOException, FeatureStoreException {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, null);
  }

  public StreamingQuery insertStream(Dataset<Row> featureData, String queryName, String outputMode,
                                     boolean awaitTermination, Long timeout, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, StreamingQueryException {
    if (!featureData.isStreaming()) {
      throw new FeatureStoreException(
          "Features have to be a streaming type spark dataframe. Use `insert()` method instead.");
    }
    LOGGER.info("StatisticsWarning: Stream ingestion for feature group `" + name + "`, with version `" + version
        + "` will not compute statistics.");
    return featureGroupEngine.insertStream(this, featureData, queryName, outputMode, awaitTermination, timeout,
        writeOptions);
  }

  public void commitDeleteRecord(Dataset<Row> featureData)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.commitDelete(this, featureData, null);
  }

  public void commitDeleteRecord(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    featureGroupEngine.commitDelete(this, featureData, writeOptions);
  }

  /**
   * Return commit details.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, null);
  }

  /**
   * Return commit details.
   *
   * @param limit number of commits to return.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetails(this, limit);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, null);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @param limit number of commits to return.
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, limit);
  }

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    if (avroSchema == null) {
      avroSchema = featureGroupEngine.getAvroSchema(this);
    }
    return avroSchema;
  }

  @JsonIgnore
  public List<String> getComplexFeatures() {
    return features.stream().filter(Feature::isComplex).map(Feature::getName).collect(Collectors.toList());
  }

  @JsonIgnore
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    Schema schema = getDeserializedAvroSchema();
    Schema.Field complexField = schema.getFields().stream().filter(field ->
        field.name().equalsIgnoreCase(featureName)).findFirst().orElseThrow(() ->
            new FeatureStoreException(
                "Complex feature `" + featureName + "` not found in AVRO schema of online feature group."));

    return complexField.schema().toString(true);
  }

  @JsonIgnore
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    Schema schema = getDeserializedAvroSchema();
    List<Schema.Field> fields = schema.getFields().stream()
        .map(field -> getComplexFeatures().contains(field.name())
            ? new Schema.Field(field.name(), SchemaBuilder.builder().nullable().bytesType(), null, null)
            : new Schema.Field(field.name(), field.schema(), null, null))
        .collect(Collectors.toList());
    return Schema.createRecord(schema.getName(), null, schema.getNamespace(), schema.isError(), fields).toString(true);
  }

  @JsonIgnore
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    try {
      return new Schema.Parser().parse(getAvroSchema());
    } catch (SchemaParseException e) {
      throw new FeatureStoreException("Failed to deserialize online feature group schema" + getAvroSchema() + ".");
    }
  }

  @JsonIgnore
  public List<String> getPrimaryKeys() {
    if (primaryKeys == null) {
      primaryKeys = features.stream().filter(f -> f.getPrimary()).map(Feature::getName).collect(Collectors.toList());
    }
    return primaryKeys;
  }

  public Expectation getExpectation(String name) throws FeatureStoreException, IOException {
    return expectationsApi.get(this, name);
  }

  @JsonIgnore
  public scala.collection.Seq<Expectation> getExpectations() throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(expectationsApi.get(this)).asScala().toSeq();
  }

  public scala.collection.Seq<Expectation> attachExpectations(scala.collection.Seq<Expectation> expectations)
      throws FeatureStoreException, IOException {
    List<Expectation> expectationsList = new ArrayList<>();
    for (Expectation expectation : (List<Expectation>) JavaConverters.seqAsJavaListConverter(expectations).asJava()) {
      expectationsList.add(attachExpectation(expectation));
    }
    return JavaConverters.asScalaBufferConverter(expectationsList).asScala().toSeq();
  }

  public Expectation attachExpectation(Expectation expectation) throws FeatureStoreException, IOException {
    return attachExpectation(expectation.getName());
  }

  public Expectation attachExpectation(String name) throws FeatureStoreException, IOException {
    return expectationsApi.put(this, name);
  }

  public void detachExpectation(Expectation expectation) throws FeatureStoreException, IOException {
    detachExpectation(expectation.getName());
  }

  public void detachExpectation(String name) throws FeatureStoreException, IOException {
    expectationsApi.detach(this, name);
  }

  public void detachExpectations(scala.collection.Seq<Expectation> expectations)
      throws FeatureStoreException, IOException {
    for (Expectation expectation : (List<Expectation>) JavaConverters.seqAsJavaListConverter(expectations).asJava()) {
      expectationsApi.detach(this, expectation);
    }
  }

  public FeatureGroupValidation validate() throws FeatureStoreException, IOException {
    // Run data validation for entire feature group
    return validate(this.read());
  }

  public FeatureGroupValidation validate(Dataset<Row> data) throws FeatureStoreException, IOException {
    // Fetch all rules
    return DataValidationEngine.getInstance().validate(data, this, expectationsApi.get(this));
  }

  @JsonIgnore
  public List<FeatureGroupValidation> getValidations() throws FeatureStoreException, IOException {
    return DataValidationEngine.getInstance().getValidations(this);
  }

  @JsonIgnore
  public FeatureGroupValidation getValidation(Long time, DataValidationEngine.ValidationTimeType type)
      throws FeatureStoreException, IOException {
    return DataValidationEngine.getInstance().getValidation(this,
      new ImmutablePair<>(type, time));
  }

  /**
   * Recompute the statistics for the feature group and save them to the feature store.
   *
   * @param wallclockTime number of commits to return.
   * @return statistics object of computed statistics
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Statistics computeStatistics(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    if (statisticsConfig.getEnabled()) {
      Map<Long, Map<String, String>> latestCommitMetaData =
          featureGroupEngine.commitDetailsByWallclockTime(this, wallclockTime, 1);
      Dataset<Row> featureData = selectAll().asOf(wallclockTime).read(false, null);
      Long commitId = (Long) latestCommitMetaData.keySet().toArray()[0];
      return statisticsEngine.computeStatistics(this, featureData, commitId);
    } else {
      LOGGER.info("StorageWarning: The statistics are not enabled of feature group `" + name + "`, with version `"
          + version + "`. No statistics computed.");
    }
    return null;
  }
}
