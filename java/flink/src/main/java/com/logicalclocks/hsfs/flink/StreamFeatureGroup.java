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
      boolean onlineEnabled, List<Feature> features, StatisticsConfig statisticsConfig,
      String onlineTopicName, String topicName, String notificationTopicName, String eventTime) {
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
    this.topicName = topicName;
    this.notificationTopicName = notificationTopicName;
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
  public DataStream<?> read(boolean online) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<?> read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void show(int numRows) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void show(int numRows, boolean online) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData) throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, Storage storage)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, boolean online, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, HudiOperationType hudiOperationType)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, Storage storage, boolean online, HudiOperationType hudiOperationType,
      Map<String, String> writeOptions) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void insert(DataStream<?> featureData, boolean online, Map<String, String> writeOptions,
      JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void commitDeleteRecord(DataStream<?> featureData) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void commitDeleteRecord(DataStream<?> featureData, Map<String, String> writeOptions)
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
  public Map<Long, Map<String, String>> commitDetails(String limit)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase selectFeatures(List<Feature> features) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase select(List<String> features) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase selectAll() {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase selectExceptFeatures(List<Feature> features) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public QueryBase selectExcept(List<String> features) {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  /**
   * Ingest a feature data to the online feature store using Flink DataStream API. Currently, only POJO
   * types as feature data type are supported.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *
   *        // get feature group handle
   *        StreamFeatureGroup fg = fs.getStreamFeatureGroup("card_transactions", 1);
   *
   *        // read stream from the source and aggregate stream
   *        DataStream<TransactionAgg> aggregationStream =
   *          env.fromSource(transactionSource, customWatermark, "Transaction Kafka Source")
   *          .keyBy(r -> r.getCcNum())
   *          .window(SlidingEventTimeWindows.of(Time.minutes(windowLength), Time.minutes(1)))
   *          .aggregate(new TransactionCountAggregate());
   *
   *        // insert streaming feature data
   *        fg.insertStream(featureData);
   * }
   * </pre>
   *
   * @param featureData Features in Streaming Dataframe to be saved.
   * @return DataStreamSink object.
   */
  @Override
  public DataStreamSink<?> insertStream(DataStream<?> featureData) throws Exception {
    return featureGroupEngine.insertStream(this, featureData, null);
  }

  @Override
  public DataStreamSink<?>  insertStream(DataStream<?> featureData, Map<String, String> writeOptions) throws Exception {
    return featureGroupEngine.insertStream(this, featureData, writeOptions);
  }

  @Override
  public Object insertStream(DataStream<?> featureData, String queryName) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> featureData, String queryName, Map<String, String> writeOptions)
      throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> featureData, String queryName, String outputMode) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> featureData, String queryName, String outputMode,
      String checkpointLocation) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> featureData,String queryName, String outputMode,
      boolean awaitTermination, Long timeout) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> featureData, String queryName, String outputMode,
      boolean awaitTermination, Long timeout, String checkpointLocation)
      throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> featureData, String queryName, String outputMode,
      boolean awaitTermination, Long timeout, String checkpointLocation,
      Map<String, String> writeOptions) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> featureData, String queryName, String outputMode, boolean awaitTermination,
      String checkpointLocation) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object insertStream(DataStream<?> featureData, String queryName, String outputMode, boolean awaitTermination,
      Long timeout, String checkpointLocation, Map<String, String> writeOptions,
      JobConfiguration jobConfiguration) throws Exception {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void updateFeatures(List<Feature> feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void appendFeatures(List<Feature> feature) throws FeatureStoreException, IOException, ParseException {
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
  public Statistics computeStatistics(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }
}
