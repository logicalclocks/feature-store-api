/*
 * Copyright (c) 2021. Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.CodeEngine;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.engine.StatisticsEngine;
import com.logicalclocks.hsfs.engine.StreamFeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.Expectation;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.StreamFeatureGroupOptions;
import com.logicalclocks.hsfs.metadata.validation.ValidationType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  private StorageConnector onlineStorageConnector;

  @Getter
  @Setter
  private StorageConnector offlineStorageConnector;

  @Getter
  @Setter
  private String type = "streamFeatureGroupDTO";

  @Getter
  @Setter
  protected String location;

  @Getter
  @Setter
  private List<String> statisticColumns;

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

  @Getter
  @Setter
  private List<StreamFeatureGroupOptions> options;

  private final StreamFeatureGroupEngine streamFeatureGroupEngine = new StreamFeatureGroupEngine();
  private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);
  private FeatureGroupUtils utils = new FeatureGroupUtils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Builder
  public StreamFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                        List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                            List<Feature> features,
                            StatisticsConfig statisticsConfig, ValidationType validationType,
                            scala.collection.Seq<Expectation> expectations, String onlineTopicName, String eventTime) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
                ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
                ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = hudiPrecombineKey != null ? hudiPrecombineKey.toLowerCase() : null;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.validationType = validationType != null ? validationType : ValidationType.NONE;
    if (expectations != null && !expectations.isEmpty()) {
      this.expectationsNames = new ArrayList<>();
      this.expectations = JavaConverters.seqAsJavaListConverter(expectations).asJava();
      this.expectations.forEach(expectation -> this.expectationsNames.add(expectation.getName()));
    }
    this.onlineTopicName = onlineTopicName;
    this.eventTime = eventTime;
  }

  // ------------------------------------------------------------------------------------------------------------------
  // TODO: (davit) Duplicated
  public Query read() throws FeatureStoreException, IOException {
    return selectAll();
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
  public Query read(String wallclockTime)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().asOf(wallclockTime);
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
  public Query readChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, IOException, ParseException {
    return selectAll().pullChanges(wallclockStartTime, wallclockEndTime);
  }

  /**
   * Get Query object to retrieve all features of the group at a point in the past.
   * This method selects all features in the feature group and returns a Query object
   * at the specified point in time. This can then either be read into a Dataframe
   * or used further to perform joins or construct a training dataset.
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, or `%Y%m%d%H%M%S`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException
   * @throws ParseException
   */
  public Query asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return selectAll().asOf(wallclockTime);
  }

  public <S> void save(S featureData, Map<String, String> writeOptions)
          throws FeatureStoreException, IOException, ParseException {
    streamFeatureGroupEngine.save(this, featureData, partitionKeys, hudiPrecombineKey, writeOptions);
    codeEngine.saveCode(this);
    if (statisticsConfig.getEnabled()) {
      statisticsEngine.computeStatistics(this, featureData, null);
    }
  }
  // ------------------------------------------------------------------------------------------------------------------

  public <S> void insert(S featureData, boolean overwrite, HudiOperationType operation,
                         SaveMode saveMode, Map<String, String> writeOptions)
          throws FeatureStoreException, IOException, ParseException {

    if (operation == null) {
      if (overwrite) {
        operation = HudiOperationType.BULK_INSERT;
      } else {
        operation = HudiOperationType.UPSERT;
      }
    }

    streamFeatureGroupEngine.insert(this, featureData, operation, saveMode, writeOptions);
    codeEngine.saveCode(this);
    computeStatistics();
  }

  public <S> Object insertStream(S featureData) {
    return insertStream(featureData, null);
  }

  public <S> Object insertStream(S featureData, String queryName) {
    return insertStream(featureData, queryName, "append");
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode) {
    return insertStream(featureData, queryName, outputMode, false, null);
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode,
                                       boolean awaitTermination, Long timeout) {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, null);
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
      Long timeout, Map<String, String> writeOptions)  {
    return streamFeatureGroupEngine.insertStream(this, featureData, queryName, outputMode,
              awaitTermination, timeout, writeOptions);
  }


  // ------------------------------------------------------------------------------------------------------------------
  // TODO: (davit) Duplicated
  public void commitDeleteRecord(Dataset<Row> featureData)
      throws FeatureStoreException, IOException, ParseException {
    utils.commitDelete(this, featureData, null);
  }

  public void commitDeleteRecord(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    utils.commitDelete(this, featureData, writeOptions);
  }

  /**
   * Return commit details.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetails(this, null);
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
    return utils.commitDetails(this, limit);
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
    return utils.commitDetailsByWallclockTime(this, wallclockTime, null);
  }

  /**
   * Return commit details.
   *
   * @param wallclockTime point in time.
   * @param limit number of commits to return.
   */
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return utils.commitDetailsByWallclockTime(this, wallclockTime, limit);
  }
  // ------------------------------------------------------------------------------------------------------------------

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    if (avroSchema == null) {
      avroSchema = streamFeatureGroupEngine.getAvroSchema(this);
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
    return Schema.createRecord(schema.getName(), null, schema.getNamespace(),
                schema.isError(), fields).toString(true);
  }

  @JsonIgnore
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    try {
      return new Schema.Parser().parse(getAvroSchema());
    } catch (SchemaParseException e) {
      throw new FeatureStoreException("Failed to deserialize online feature group schema" + getAvroSchema() + ".");
    }
  }
}
