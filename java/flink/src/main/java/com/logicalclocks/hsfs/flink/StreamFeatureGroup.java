/*
 *  Copyright (c) 2022-2022. Hopsworks AB
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

package com.logicalclocks.hsfs.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.JobConfiguration;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.constructor.QueryBase;

import com.logicalclocks.hsfs.metadata.Statistics;

import com.logicalclocks.hsfs.flink.engine.FeatureGroupEngine;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase<DataStream<?>> {

  protected FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

  @Builder
  public StreamFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
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

  public StreamFeatureGroup(FeatureStore featureStore, int id) {
    this();
    this.featureStore = featureStore;
    this.id = id;
  }

  @Override
  public DataStream<?> read() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(boolean b) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(Map<String, String> map) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(boolean b, Map<String, String> map) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(String s) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(String s, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase asOf(String s) throws FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase asOf(String s, String s1) throws FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void show(int i) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void show(int i, boolean b) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream) throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, Storage storage)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, boolean b) throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, Storage storage, boolean b)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, boolean b, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, HudiOperationType hudiOperationType)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, Storage storage, boolean b, HudiOperationType hudiOperationType,
      Map<String, String> map) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> dataStream, boolean b, Map<String, String> map, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void commitDeleteRecord(DataStream<?> dataStream) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void commitDeleteRecord(DataStream<?> dataStream, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
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
  public Map<Long, Map<String, String>> commitDetails(String s)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String s, Integer integer)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase selectFeatures(List<Feature> list) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase select(List<String> list) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase selectAll() {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase selectExceptFeatures(List<Feature> list) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase selectExcept(List<String> list) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStreamSink<?> insertStream(DataStream<?> dataStream) throws Exception {
    return featureGroupEngine.insertStream(this, dataStream);
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, String s) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, Map<String, String> map) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, String s, Map<String, String> map) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, String s, String s1) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, String s, String s1, String s2) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream,String queryName, String outputMode,
      boolean awaitTermination, Long timeout) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, String s, String s1, boolean b, Long timeout, String s2)
      throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, String s, String s1, boolean b, Long timeout, String s2,
      Map<String, String> map) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, String s, String s1, boolean b, String s2) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> dataStream, String s, String s1, boolean b, Long timeout, String s2,
      Map<String, String> map, JobConfiguration jobConfiguration) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void updateFeatures(List<Feature> list) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void appendFeatures(List<Feature> list) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void appendFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Statistics computeStatistics() throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Statistics computeStatistics(String s) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }
}
