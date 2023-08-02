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

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.beam.constructor.Query;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

public class FeatureView extends FeatureViewBase<FeatureView, FeatureStore, Query, PCollection<Object>> {
  @Override
  public void addTag(String name, Object value) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Map<String, Object> getTags() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object getTag(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void deleteTag(String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void addTrainingDatasetTag(Integer version, String name, Object value)
      throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Map<String, Object> getTrainingDatasetTags(Integer version) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object getTrainingDatasetTag(Integer version, String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void deleteTrainingDatasetTag(Integer version, String name) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void delete() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void clean(FeatureStore featureStore, String featureViewName, Integer featureViewVersion)
      throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public FeatureView update(FeatureView other) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public String getBatchQuery() throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public String getBatchQuery(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public PCollection<Object> getBatchData() throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public PCollection<Object> getBatchData(String startTime, String endTime)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public PCollection<Object> getBatchData(String startTime, String endTime, Map<String, String> readOptions)
      throws FeatureStoreException, IOException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object getTrainingData(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object getTrainTestSplit(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object getTrainValidationTestSplit(Integer version, Map<String, String> readOptions)
      throws IOException, FeatureStoreException, ParseException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void purgeTrainingData(Integer version) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void purgeAllTrainingData() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void deleteTrainingDataset(Integer version) throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public void deleteAllTrainingDatasets() throws FeatureStoreException, IOException {
    throw new UnsupportedOperationException("Not supported for Beam");
  }
}
