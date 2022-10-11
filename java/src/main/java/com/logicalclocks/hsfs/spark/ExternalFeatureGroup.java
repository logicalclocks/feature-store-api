/*
 *  Copyright (c) 2020-2022. Logical Clocks AB
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

package com.logicalclocks.hsfs.spark;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.generic.EntityEndpointType;
import com.logicalclocks.hsfs.generic.ExternalDataFormat;
import com.logicalclocks.hsfs.generic.Feature;
import com.logicalclocks.hsfs.generic.FeatureStore;
import com.logicalclocks.hsfs.generic.FeatureStoreException;
import com.logicalclocks.hsfs.generic.StatisticsConfig;
import com.logicalclocks.hsfs.generic.StorageConnector;
import com.logicalclocks.hsfs.generic.TimeTravelFormat;
import com.logicalclocks.hsfs.spark.constructor.Query;
import com.logicalclocks.hsfs.spark.engine.ExternalFeatureGroupEngine;
import com.logicalclocks.hsfs.generic.engine.CodeEngine;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.generic.metadata.OnDemandOptions;
import com.logicalclocks.hsfs.spark.engine.StatisticsEngine;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalFeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  private com.logicalclocks.hsfs.generic.StorageConnector storageConnector;

  @Getter
  @Setter
  private String query;

  @Getter
  @Setter
  private ExternalDataFormat dataFormat;

  @Getter
  @Setter
  private String path;

  @Getter
  @Setter
  private List<OnDemandOptions> options;

  @Getter
  @Setter
  private String type = "onDemandFeaturegroupDTO";

  private ExternalFeatureGroupEngine externalFeatureGroupEngine = new ExternalFeatureGroupEngine();
  private final StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.FEATURE_GROUP);
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);

  @Builder
  public ExternalFeatureGroup(com.logicalclocks.hsfs.generic.FeatureStore featureStore, @NonNull String name, Integer version, String query,
                              ExternalDataFormat dataFormat, String path, Map<String, String> options,
                              @NonNull StorageConnector storageConnector, String description, List<String> primaryKeys,
                              List<Feature> features, StatisticsConfig statisticsConfig, String eventTime) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.query = query;
    this.dataFormat = dataFormat;
    this.path = path;
    this.options = options != null ? options.entrySet().stream()
        .map(e -> new OnDemandOptions(e.getKey(), e.getValue()))
        .collect(Collectors.toList())
        : null;
    this.description = description;
    this.primaryKeys = primaryKeys != null
            ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.storageConnector = storageConnector;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.eventTime = eventTime;
  }

  public ExternalFeatureGroup() {
  }

  public ExternalFeatureGroup(FeatureStore featureStore, int id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  public void save() throws FeatureStoreException, IOException {
    externalFeatureGroupEngine.saveFeatureGroup(this);
    codeEngine.saveCode(this);
    if (statisticsConfig.getEnabled()) {
      statisticsEngine.computeStatistics(this, read(), null);
    }
  }

  @Override
  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return selectAll().read();
  }


  public void show(int numRows) throws FeatureStoreException, IOException {
    read().show(numRows);
  }

  @Override
  public Query selectAll() {
    return new Query(this, getFeatures());
  }

  @Override
  public Query selectFeatures(List<Feature> features) {
    return null;
  }

  @Override
  public Query selectExceptFeatures(List<Feature> features) {
    return null;
  }

  @Override
  public Query selectExcept(List<String> features) {
    return null;
  }

  @Override
  public void updateFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void appendFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void appendFeatures(Feature features) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public <T> T computeStatistics() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public String getOnlineTopicName() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public List<String> getComplexFeatures() {
    return null;
  }

  @Override
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public TimeTravelFormat getTimeTravelFormat() {
    return null;
  }
}
