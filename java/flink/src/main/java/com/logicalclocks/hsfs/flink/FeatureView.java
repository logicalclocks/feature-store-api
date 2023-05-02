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

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.flink.constructor.Query;

import org.apache.flink.streaming.api.datastream.DataStream;

import lombok.NoArgsConstructor;

import java.io.IOException;
import java.text.ParseException;

import java.util.Map;

@NoArgsConstructor
public class FeatureView extends FeatureViewBase<FeatureView, FeatureStore, Query, DataStream<?>> {

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
  public DataStream<Object> getBatchData() throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<Object> getBatchData(String s, String s1)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Flink");
  }

  @Override
  public DataStream<Object> getBatchData(String s, String s1, Map<String, String> map)
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
