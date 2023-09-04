/*
 *  Copyright (c) 2023. Hopsworks AB
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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.logicalclocks.hsfs.engine.FeatureGroupEngineBase;
import com.logicalclocks.hsfs.metadata.Statistics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase<List<Object>> {

  protected FeatureGroupEngineBase featureGroupEngine = new FeatureGroupEngineBase();

  @Builder
  public StreamFeatureGroup(FeatureStoreBase featureStore, @NonNull String name, Integer version, String description,
      List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
      boolean onlineEnabled, List<Feature> features,
      StatisticsConfig statisticsConfig, String onlineTopicName, String eventTime) {
    this();
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

  public StreamFeatureGroup() {
    this.type = "streamFeatureGroupDTO";
  }

  // used for updates
  public StreamFeatureGroup(Integer id, String description, List<Feature> features) {
    this();
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public StreamFeatureGroup(FeatureStoreBase featureStore, int id) {
    this();
    this.featureStore = featureStore;
    this.id = id;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(Integer integer)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String limit)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public List<Object> insertStream(List<Object> featureData) throws Exception {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public List<Object>  insertStream(List<Object> featureData, Map<String, String> writeOptions) throws Exception {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void updateFeatures(List<Feature> feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void appendFeatures(List<Feature> feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void appendFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not implemented.");
  }
}
