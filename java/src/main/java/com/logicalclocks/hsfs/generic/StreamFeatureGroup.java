/*
 *  Copyright (c) 2022. Logical Clocks AB
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

package com.logicalclocks.hsfs.generic;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.logicalclocks.hsfs.generic.engine.CodeEngine;
import com.logicalclocks.hsfs.generic.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.generic.engine.StreamFeatureGroupEngine;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupBase;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  private Boolean onlineEnabled;

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

  @Getter(onMethod = @__(@Override))
  @Setter
  private String onlineTopicName;

  @Setter
  private DeltaStreamerJobConf deltaStreamerJobConf;

  private final StreamFeatureGroupEngine streamFeatureGroupEngine = new StreamFeatureGroupEngine();
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);
  private FeatureGroupUtils utils = new FeatureGroupUtils();

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamFeatureGroup.class);

  @Builder
  public StreamFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                            List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                            boolean onlineEnabled, List<Feature> features,
                            StatisticsConfig statisticsConfig, String onlineTopicName, String eventTime) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
        ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
        ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = hudiPrecombineKey != null ? hudiPrecombineKey.toLowerCase() : null;
    this.onlineEnabled = onlineEnabled;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.onlineTopicName = onlineTopicName;
    this.eventTime = eventTime;
  }

  // used for updates
  public StreamFeatureGroup(Integer id, String description, List<Feature> features) {
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public StreamFeatureGroup(FeatureStore featureStore, int id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  public <S> void insert(S featureData) throws FeatureStoreException, IOException, ParseException {
    insert(featureData, false, SaveMode.APPEND, null, null);
  }

  public <S> void insert(S featureData, Map<String, String> writeOptions) throws FeatureStoreException, IOException,
      ParseException {
    insert(featureData, false, SaveMode.APPEND, writeOptions, null);
  }

  public <S> void insert(S featureData, JobConfiguration jobConfiguration) throws FeatureStoreException, IOException,
      ParseException {
    insert(featureData, false, SaveMode.APPEND, null, jobConfiguration);
  }

  public <S> void insert(S featureData, boolean overwrite, SaveMode saveMode,
                         Map<String, String> writeOptions) throws FeatureStoreException, IOException, ParseException {
    insert(featureData, false, SaveMode.APPEND, writeOptions, null);
  }

  public <S> void insert(S featureData, boolean overwrite, SaveMode saveMode,
                         JobConfiguration jobConfiguration) throws FeatureStoreException, IOException, ParseException {
    insert(featureData, false, SaveMode.APPEND, jobConfiguration);
  }

  public <S> void insert(S featureData, boolean overwrite, SaveMode saveMode,
                         Map<String, String> writeOptions, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

    streamFeatureGroupEngine.insert(this, featureData, saveMode,  partitionKeys,
        hudiPrecombineKey, writeOptions, jobConfiguration);
    codeEngine.saveCode(this);
  }

  public <S> Object insertStream(S featureData) {
    return insertStream(featureData, null, "append", false, null, null, null);
  }

  public <S> Object insertStream(S featureData, String queryName) {
    return insertStream(featureData, queryName, null,false, null, null, null);
  }

  public <S> Object insertStream(S featureData, Map<String, String> writeOptions) {
    return insertStream(featureData, null, null, false, null, null, writeOptions);
  }

  public <S> Object insertStream(S featureData, String queryName, Map<String, String> writeOptions) {
    return insertStream(featureData, queryName, "append", false, null, null, writeOptions);
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode) {
    return insertStream(featureData, queryName, outputMode, false, null, null, null);
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode, String checkpointLocation) {
    return insertStream(featureData, queryName, outputMode, false, null, checkpointLocation, null);
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
                                 Long timeout) {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, null, null);
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
                                 Long timeout, String checkpointLocation) {
    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, checkpointLocation, null);
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
                                 Long timeout,  String checkpointLocation, Map<String, String> writeOptions) {

    return insertStream(featureData, queryName, outputMode, awaitTermination, timeout, checkpointLocation, writeOptions,
        null);
  }

  public <S> Object insertStream(S featureData, String queryName, String outputMode, boolean awaitTermination,
                                 Long timeout,  String checkpointLocation, Map<String, String> writeOptions,
                                 JobConfiguration jobConfiguration) {

    return streamFeatureGroupEngine.insertStream(this, featureData, queryName, outputMode,
        awaitTermination, timeout, checkpointLocation,  partitionKeys, hudiPrecombineKey, writeOptions,
        jobConfiguration);
  }

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    if (avroSchema == null) {
      avroSchema = utils.getAvroSchema(this);
    }
    return avroSchema;
  }

  @JsonIgnore
  public List<String> getComplexFeatures() {
    return utils.getComplexFeatures(features);
  }

  @JsonIgnore
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    return utils.getFeatureAvroSchema(featureName, utils.getDeserializedAvroSchema(getAvroSchema()));
  }

  @JsonIgnore
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getEncodedAvroSchema(getDeserializedAvroSchema(), utils.getComplexFeatures(features));
  }

  @JsonIgnore
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getDeserializedAvroSchema(getAvroSchema());
  }
}
