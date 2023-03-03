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

package com.logicalclocks.base;

import com.logicalclocks.base.constructor.QueryBase;
import com.logicalclocks.base.metadata.Statistics;

import lombok.NoArgsConstructor;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class FeatureGroupBaseForApi extends FeatureGroupBase {

  public FeatureGroupBaseForApi(FeatureStoreBase featureStore, Integer id) {
    this.featureStore = featureStore;
    this.id = id;
  }

  @Override
  public Object read() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object read(boolean online) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object read(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public Object read(String wallclockTime, Map readOptions) throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public Object read(boolean online, Map readOptions) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Object read(Map readOptions) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public QueryBase asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public QueryBase asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public void show(int numRows) throws FeatureStoreException, IOException {

  }

  @Override
  public void show(int numRows, boolean online) throws FeatureStoreException, IOException {

  }

  @Override
  public void insert(Object featureData) throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(Object featureData, Storage storage) throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(Object featureData, boolean overwrite) throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(Object featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException, ParseException {

  }

  @Override
  public void insert(Object featureData, HudiOperationType operation)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Object featureData, boolean overwrite, Map writeOptions, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Object featureData, JobConfiguration jobConfiguration)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Object featureData, Storage storage, boolean overwrite, HudiOperationType operation,
                     Map writeOptions) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Object featureData, boolean overwrite, Map writeOptions)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void insert(Object featureData, Map writeOptions) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public Map<Long, Map<String, String>> commitDetails() throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime)
      throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public Map<Long, Map<String, String>> commitDetails(String wallclockTime, Integer limit)
      throws IOException, FeatureStoreException, ParseException {
    return null;
  }

  @Override
  public QueryBase selectAll() {
    return null;
  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void updateFeatures(List list) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void appendFeatures(Feature features) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void appendFeatures(List list) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public Statistics computeStatistics() throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public Statistics computeStatistics(String wallclockTime) throws FeatureStoreException, IOException, ParseException {
    return null;
  }

  @Override
  public Statistics getStatistics() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public QueryBase selectExcept(List features) {
    return null;
  }

  @Override
  public QueryBase selectExceptFeatures(List list) {
    return null;
  }

  @Override
  public QueryBase select(List features) {
    return null;
  }

  @Override
  public QueryBase selectFeatures(List list) {
    return null;
  }

  @Override
  public void commitDeleteRecord(Object featureData, Map writeOptions)
      throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void commitDeleteRecord(Object featureData) throws FeatureStoreException, IOException, ParseException {

  }
}
