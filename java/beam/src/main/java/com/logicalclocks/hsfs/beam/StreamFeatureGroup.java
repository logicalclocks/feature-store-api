package com.logicalclocks.hsfs.beam;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.JobConfiguration;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.beam.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.beam.engine.HsfsBeamProducer;
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

  @Override
  public PCollection<Object> read() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public PCollection<Object> read(boolean b) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public PCollection<Object> read(Map<String, String> map) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public PCollection<Object> read(boolean b, Map<String, String> map) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public PCollection<Object> read(String s) throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public PCollection<Object> read(String s, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public QueryBase asOf(String s) throws FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public QueryBase asOf(String s, String s1) throws FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public void show(int i) throws FeatureStoreException, IOException {

  }

  @Override
  public void show(int i, boolean b) throws FeatureStoreException, IOException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection) throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, Storage storage)
      throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, boolean b)
      throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, Storage storage, boolean b)
      throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, boolean b, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, HudiOperationType hudiOperationType)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, Storage storage, boolean b,
      HudiOperationType hudiOperationType, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(PCollection<Object> objectPCollection, boolean b, Map<String, String> map,
      JobConfiguration jobConfiguration) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void commitDeleteRecord(PCollection<Object> objectPCollection)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void commitDeleteRecord(PCollection<Object> objectPCollection, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(Integer integer)
      throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String s)
      throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String s, Integer integer)
      throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public QueryBase selectFeatures(List<Feature> list) {
    return null;
  }

  @Override
  public QueryBase select(List<String> list) {
    return null;
  }

  @Override
  public QueryBase selectAll() {
    return null;
  }

  @Override
  public QueryBase selectExceptFeatures(List<Feature> list) {
    return null;
  }

  @Override
  public QueryBase selectExcept(List<String> list) {
    return null;
  }

  public HsfsBeamProducer insertStream() throws Exception {
    return featureGroupEngine.insertStream(this);
  }


  @Override
  public Object insertStream(PCollection<Object> objectPCollection) throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s) throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, Map<String, String> map) throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s, Map<String, String> map)
      throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s, String s1) throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s, String s1, String s2) throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s, String s1, boolean b, Long timeOut)
      throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s, String s1, boolean b, Long timeout,
      String s2) throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s, String s1, boolean b, Long timeout,
      String s2, Map<String, String> map) throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s, String s1, boolean b, String s2)
      throws Exception {
    return null;
  }

  @Override
  public Object insertStream(PCollection<Object> objectPCollection, String s, String s1, boolean b, Long timeout,
      String s2, Map<String, String> map, JobConfiguration jobConfiguration) throws Exception {
    return null;
  }

  @Override
  public void updateFeatures(List<Feature> list) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void appendFeatures(List<Feature> list) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void appendFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public Statistics computeStatistics() throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public Statistics computeStatistics(String s) throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    return null;
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



}
