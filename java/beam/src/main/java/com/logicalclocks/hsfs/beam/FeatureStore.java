package com.logicalclocks.hsfs.beam;

import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.TrainingDatasetBase;
import com.logicalclocks.hsfs.beam.constructor.Query;
import com.logicalclocks.hsfs.beam.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;

public class FeatureStore extends FeatureStoreBase<Query> {

  private FeatureGroupEngine featureGroupEngine;

  public FeatureStore() {
    storageConnectorApi = new StorageConnectorApi();
    featureGroupEngine = new FeatureGroupEngine();
  }

  @Override
  public Object createFeatureGroup() {
    return null;
  }

  @Override
  public Object getFeatureGroups(@NonNull String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getOrCreateFeatureGroup(String s, Integer integer) throws IOException, FeatureStoreException {
    return null;
  }

  @Override
  public Object getOrCreateFeatureGroup(String s, Integer integer, List<String> list, List<String> list1, boolean b,
      String s1) throws IOException, FeatureStoreException {
    return null;
  }

  @Override
  public Object getOrCreateFeatureGroup(String s, Integer integer, List<String> list, boolean b, String s1)
      throws IOException, FeatureStoreException {
    return null;
  }

  @Override
  public Object getOrCreateFeatureGroup(String s, Integer integer, String s1, List<String> list, List<String> list1,
      String s2, boolean b, TimeTravelFormat timeTravelFormat, StatisticsConfig statisticsConfig, String s3)
      throws IOException, FeatureStoreException {
    return null;
  }

  /**
   * Get a feature group object from the feature store.
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupEngine.getStreamFeatureGroup(this, name, version);
  }

  @Override
  public Object getStreamFeatureGroup(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object createStreamFeatureGroup() {
    return null;
  }

  @Override
  public Object getOrCreateStreamFeatureGroup(String s, Integer integer) throws IOException, FeatureStoreException {
    return null;
  }

  @Override
  public Object getOrCreateStreamFeatureGroup(String s, Integer integer, List<String> list, boolean b, String s1)
      throws IOException, FeatureStoreException {
    return null;
  }

  @Override
  public Object getOrCreateStreamFeatureGroup(String s, Integer integer, List<String> list, List<String> list1,
      boolean b, String s1) throws IOException, FeatureStoreException {
    return null;
  }

  @Override
  public Object getOrCreateStreamFeatureGroup(String s, Integer integer, String s1, List<String> list,
      List<String> list1, String s2, boolean b, StatisticsConfig statisticsConfig, String s3)
      throws IOException, FeatureStoreException {
    return null;
  }

  @Override
  public Object createExternalFeatureGroup() {
    return null;
  }

  @Override
  public Object createFeatureView() {
    return null;
  }

  @Override
  public Object getFeatureView(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getFeatureView(@NonNull String s, @NonNull Integer integer) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getOrCreateFeatureView(String s, Query query, Integer integer)
      throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getOrCreateFeatureView(String s, Query query, Integer integer, String s1, List<String> list)
      throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getExternalFeatureGroup(@NonNull String s, @NonNull Integer integer)
      throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getExternalFeatureGroup(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public StorageConnector getStorageConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getHopsFsConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getExternalFeatureGroups(@NonNull String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object sql(String s) {
    return null;
  }

  @Override
  public Object getJdbcConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getS3Connector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getRedshiftConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getSnowflakeConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getAdlsConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getKafkaConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getBigqueryConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getOnlineStorageConnector() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getGcsConnector(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public TrainingDatasetBase getTrainingDataset(@NonNull String s, @NonNull Integer integer)
      throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public TrainingDatasetBase getTrainingDataset(String s) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object getTrainingDatasets(@NonNull String s) throws FeatureStoreException, IOException {
    return null;
  }
}
