package com.logicalclocks.hsfs;

import com.logicalclocks.hsfs.constructor.Query;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class FeatureView extends FeatureViewBase<FeatureView, FeatureStore, Query, List<Object>> {

  @Override
  public void addTag(String s, Object o) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Map<String, Object> getTags() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getTag(String s) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void deleteTag(String s) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void addTrainingDatasetTag(Integer integer, String s, Object o) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Map<String, Object> getTrainingDatasetTags(Integer integer) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getTrainingDatasetTag(Integer integer, String s) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void deleteTrainingDatasetTag(Integer integer, String s) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void delete() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void clean(FeatureStore featureStore, String s, Integer integer) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public FeatureView update(FeatureView featureView) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public String getBatchQuery() throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public String getBatchQuery(String s, String s1) throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public List<Object> getBatchData() throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public List<Object> getBatchData(String s, String s1)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public List<Object> getBatchData(String s, String s1, Map<String, String> map)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getTrainingData(Integer integer, Map<String, String> map)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getTrainTestSplit(Integer integer, Map<String, String> map)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public Object getTrainValidationTestSplit(Integer integer, Map<String, String> map)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void purgeTrainingData(Integer integer) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void purgeAllTrainingData() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void deleteTrainingDataset(Integer integer) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public void deleteAllTrainingDatasets() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }
}
