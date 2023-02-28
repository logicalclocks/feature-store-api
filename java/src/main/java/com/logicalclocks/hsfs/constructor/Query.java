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

package com.logicalclocks.hsfs.constructor;

import com.logicalclocks.base.Feature;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.Storage;
import com.logicalclocks.base.constructor.Filter;
import com.logicalclocks.base.constructor.FilterLogic;
import com.logicalclocks.base.constructor.Join;
import com.logicalclocks.base.constructor.JoinType;
import com.logicalclocks.base.constructor.QueryBase;
import com.logicalclocks.base.metadata.FeatureGroupBase;
import com.logicalclocks.base.metadata.QueryConstructorApi;
import com.logicalclocks.base.metadata.StorageConnectorApi;

import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.engine.SparkEngine;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class Query extends QueryBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

  private QueryConstructorApi queryConstructorApi = new QueryConstructorApi();
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();

  public Query(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    this.leftFeatureGroup = leftFeatureGroup;
    this.leftFeatures = leftFeatures;
  }

  @Override
  public String sql() {
    // overriding toString does not work wtih jackson
    return sql(Storage.OFFLINE);
  }

  @Override
  public String sql(Storage storage) {
    try {
      return queryConstructorApi
          .constructQuery(this.getLeftFeatureGroup().getFeatureStore(), this, FsQuery.class)
          .getStorageQuery(storage);
    } catch (FeatureStoreException | IOException e) {
      return e.getMessage();
    }
  }

  public Query join(Query subquery) {
    return join(subquery, JoinType.INNER);
  }

  public Query join(Query subquery, String prefix) {
    return join(subquery, JoinType.INNER, prefix);
  }

  public Query join(Query subquery, List<String> on) {
    return joinFeatures(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public Query join(Query subquery, List<String> leftOn, List<String> rightOn) {
    return joinFeatures(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public Query join(Query subquery, List<String> leftOn, List<String> rightOn, String prefix) {
    return joinFeatures(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER, prefix);
  }

  public Query join(Query subquery, JoinType joinType) {
    joins.add(new Join(subquery, joinType, null));
    return this;
  }

  public Query join(Query subquery, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, joinType, prefix));
    return this;
  }

  public Query join(Query subquery, List<String> on, JoinType joinType) {
    joins.add(new Join(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), joinType, null));
    return this;
  }

  public Query join(Query subquery, List<String> on, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), joinType, prefix));
    return this;
  }

  public Query join(Query subquery, List<String> leftOn, List<String> rightOn, JoinType joinType) {
    joins.add(new Join(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), joinType, null));
    return this;
  }

  public Query join(Query subquery, List<String> leftOn, List<String> rightOn, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), joinType, prefix));
    return this;
  }

  public Query joinFeatures(
      Query subquery, List<Feature> on) {
    return joinFeatures(subquery, on, JoinType.INNER);
  }

  public Query joinFeatures(Query subquery, List<Feature> on, String prefix) {
    return joinFeatures(subquery, on, JoinType.INNER, prefix);
  }

  public Query joinFeatures(Query subquery, List<Feature> leftOn, List<Feature> rightOn) {
    return joinFeatures(subquery, leftOn, rightOn, JoinType.INNER);
  }

  public Query joinFeatures(Query subquery, List<Feature> leftOn, List<Feature> rightOn, String prefix) {
    return joinFeatures(subquery, leftOn, rightOn, JoinType.INNER, prefix);
  }

  public Query joinFeatures(Query subquery, List<Feature> on, JoinType joinType) {
    joins.add(new Join(subquery, on, joinType, null));
    return this;
  }

  public Query joinFeatures(Query subquery, List<Feature> on, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, on, joinType, prefix));
    return this;
  }

  public Query joinFeatures(Query subquery, List<Feature> leftOn, List<Feature> rightOn, JoinType joinType) {
    joins.add(new Join(subquery, leftOn, rightOn, joinType, null));
    return this;
  }

  public Query joinFeatures(Query subquery, List<Feature> leftOn, List<Feature> rightOn, JoinType joinType,
      String prefix) {
    joins.add(new Join(subquery, leftOn, rightOn, joinType, prefix));
    return this;
  }

  /**
   * Perform time travel on the given Query.
   * This method returns a new Query object at the specified point in time.
   * This can then either be read into a Dataframe or used further to perform joins
   * or construct a training dataset.
   *
   * @param wallclockTime point in time
   * @return Query
   * @throws FeatureStoreException FeatureStoreException
   * @throws ParseException ParseException
   */
  public Query asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return asOf(wallclockTime, null);
  }

  /**
   * Perform time travel on the given Query.
   * This method returns a new Query object at the specified point in time.
   * This can then either be read into a Dataframe or used further to perform joins
   * or construct a training dataset.
   *
   * @param wallclockTime point in time
   * @param excludeUntil point in time
   * @return Query
   * @throws FeatureStoreException FeatureStoreException
   * @throws ParseException ParseException
   */
  public Query asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    return (Query) super.genericAsOf(wallclockTime, excludeUntil);
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return Query
   * @throws FeatureStoreException FeatureStoreException
   * @throws ParseException ParseException
   *
   * @deprecated use asOf(wallclockEndTime, wallclockStartTime) instead
   */
  public Query pullChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, ParseException {
    return (Query) super.genericPullChanges(wallclockStartTime, wallclockEndTime);
  }

  public Query filter(Filter filter) {
    return (Query) super.genericFilter(filter);
  }

  public Query filter(FilterLogic filter) {
    return (Query) super.genericFilter(filter);
  }

  public Query appendFeature(Feature feature) {
    this.leftFeatures.add(feature);
    return this;
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return read(online, null);
  }

  @Override
  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    FsQuery fsQuery = (FsQuery)
        queryConstructorApi.constructQuery(leftFeatureGroup.getFeatureStore(), this, FsQuery.class);

    if (online) {
      LOGGER.info("Executing query: " + fsQuery.getStorageQuery(Storage.ONLINE));
      StorageConnector.JdbcConnector onlineConnector =
          storageConnectorApi.getOnlineStorageConnector(
              leftFeatureGroup.getFeatureStore(), StorageConnector.JdbcConnector.class);
      return onlineConnector.read(fsQuery.getStorageQuery(Storage.ONLINE),null, null, null);
    } else {
      fsQuery.registerOnDemandFeatureGroups();
      fsQuery.registerHudiFeatureGroups(readOptions);

      LOGGER.info("Executing query: " + fsQuery.getStorageQuery(Storage.OFFLINE));
      return SparkEngine.getInstance().sql(fsQuery.getStorageQuery(Storage.OFFLINE));
    }
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    show(false, numRows);
  }

  @Override
  public void show(boolean online, int numRows) throws FeatureStoreException, IOException {
    SparkEngine.getInstance().objectToDataset(read(online)).show(numRows);
  }
}
