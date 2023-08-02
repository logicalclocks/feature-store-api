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

package com.logicalclocks.hsfs.beam;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.JobConfiguration;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.beam.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.beam.engine.BeamProducer;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.metadata.Statistics;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamFeatureGroup extends FeatureGroupBase<PCollection<Object>> {


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
  public PCollection<Object> read() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public PCollection<Object> read(boolean online) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public PCollection<Object> read(Map<String, String> readOptions) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public PCollection<Object> read(boolean online, Map<String, String> readOptions)
      throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public PCollection<Object> read(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public PCollection<Object> read(String wallclockTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public QueryBase asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public QueryBase asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void show(int numRows) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void show(int numRows, boolean online) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData) throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, Storage storage)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, boolean online, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, HudiOperationType hudiOperationType)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, Storage storage, boolean online,
      HudiOperationType hudiOperationType, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void insert(PCollection<Object> featureData, boolean online, Map<String, String> writeOptions,
      JobConfiguration jobConfiguration) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void commitDeleteRecord(PCollection<Object> featureData)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void commitDeleteRecord(PCollection<Object> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(Integer integer)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String limit)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public QueryBase selectFeatures(List<Feature> features) {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public QueryBase select(List<String> features) {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public QueryBase selectAll() {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public QueryBase selectExceptFeatures(List<Feature> features) {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public QueryBase selectExcept(List<String> features) {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  /**
   * Ingest a feature data to the online feature store using Beam Pipeline object. Currently,
   * only org.apache.beam.sdk.values.Row types as feature data type are supported.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *
   *        // get feature group handle
   *        StreamFeatureGroup fg = fs.getStreamFeatureGroup("taxi_ride", 1);
   *
   *        // create Beam pipeline
   *        Pipeline pipeline = Pipeline.create();
   *        pipeline
   *         .apply("read stream from the source", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
   *         .apply("Parse JSON to Beam Rows", JsonToRow.withSchema(schema))
   *         .apply("insert streaming feature data", fg.insertStream());
   * }
   * </pre>
   *
   * @return BeamProducer object, that can be wrapped inside Beam Pipeline `apply` method.
   */
  public BeamProducer insertStream() throws Exception {
    return featureGroupEngine.insertStream(this, null);
  }

  public BeamProducer insertStream(Map<String, String> writeOptions) throws Exception {
    return featureGroupEngine.insertStream(this, writeOptions);
  }

  @Override
  public Object insertStream(PCollection<Object> featureData) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, String queryName) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, Map<String, String> writeOptions) throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, String queryName, Map<String, String> writeOptions)
      throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, String queryName, String outputMode) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, String queryName, String outputMode,
      String checkpointLocation) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData,String queryName, String outputMode,
      boolean awaitTermination, Long timeout) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, String queryName, String outputMode,
      boolean awaitTermination, Long timeout, String checkpointLocation) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, String queryName, String outputMode,
      boolean awaitTermination, Long timeout, String checkpointLocation, Map<String, String> writeOptions)
      throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, String queryName, String outputMode,
      boolean awaitTermination, String checkpointLocation) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, String queryName, String outputMode,
      boolean awaitTermination, Long timeout, String checkpointLocation, Map<String, String> writeOptions,
      JobConfiguration jobConfiguration) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void updateFeatures(List<Feature> feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void appendFeatures(List<Feature> feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void appendFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Statistics computeStatistics() throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Statistics computeStatistics(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }
}
