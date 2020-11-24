/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.metadata;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FsQuery;
import com.logicalclocks.hsfs.JoinType;
import com.logicalclocks.hsfs.OnDemandFeatureGroup;
import com.logicalclocks.hsfs.OnDemandFeatureGroupAlias;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.HudiFeatureGroupAlias;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Query {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Getter
  @Setter
  private FeatureGroupBase leftFeatureGroup;
  @Getter
  @Setter
  private List<Feature> leftFeatures;
  @Getter
  @Setter
  private String leftFeatureGroupStartTime;
  @Getter
  @Setter
  private String leftFeatureGroupEndTime;

  @Getter
  @Setter
  private List<Join> joins = new ArrayList<>();

  private QueryConstructorApi queryConstructorApi;
  private StorageConnectorApi storageConnectorApi;

  public Query(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    this.leftFeatureGroup = leftFeatureGroup;
    this.leftFeatures = leftFeatures;

    this.queryConstructorApi = new QueryConstructorApi();
    this.storageConnectorApi = new StorageConnectorApi();
  }

  public Query join(Query subquery) {
    return join(subquery, JoinType.INNER);
  }

  public Query join(Query subquery, List<String> on) {
    return joinFeatures(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public Query join(Query subquery, List<String> leftOn, List<String> rightOn) {
    return joinFeatures(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public Query join(Query subquery, JoinType joinType) {
    joins.add(new Join(subquery, joinType));
    return this;
  }

  public Query join(Query subquery, List<String> on, JoinType joinType) {
    joins.add(new Join(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), joinType));
    return this;
  }

  public Query join(Query subquery, List<String> leftOn, List<String> rightOn, JoinType joinType) {
    joins.add(new Join(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), joinType));
    return this;
  }

  public Query joinFeatures(Query subquery, List<Feature> on) {
    return joinFeatures(subquery, on, JoinType.INNER);
  }

  public Query joinFeatures(Query subquery, List<Feature> leftOn, List<Feature> rightOn) {
    return joinFeatures(subquery, leftOn, rightOn, JoinType.INNER);
  }

  public Query joinFeatures(Query subquery, List<Feature> on, JoinType joinType) {
    joins.add(new Join(subquery, on, joinType));
    return this;
  }

  public Query joinFeatures(Query subquery, List<Feature> leftOn, List<Feature> rightOn, JoinType joinType) {
    joins.add(new Join(subquery, leftOn, rightOn, joinType));
    return this;
  }

  /**
   * Reads Feature group data at a specific point in time.
   *
   * @param wallclockTime point in time
   * @return Query
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Query asOf(String wallclockTime) {
    for (Join join : this.joins) {
      Query queryWithTimeStamp = join.getQuery();
      queryWithTimeStamp.setLeftFeatureGroupEndTime(wallclockTime);
      join.setQuery(queryWithTimeStamp);
    }
    this.setLeftFeatureGroupEndTime(wallclockTime);
    return this;
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return Query
   * @throws FeatureStoreException
   * @throws IOException
   */
  public Query pullChanges(String wallclockStartTime, String wallclockEndTime) {
    this.setLeftFeatureGroupStartTime(wallclockStartTime);
    this.setLeftFeatureGroupEndTime(wallclockEndTime);
    return this;
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return read(online, null);
  }

  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    FsQuery fsQuery = queryConstructorApi.constructQuery(leftFeatureGroup.getFeatureStore(), this);

    if (online) {
      LOGGER.info("Executing query: " + fsQuery.getStorageQuery(Storage.ONLINE));
      StorageConnector onlineConnector =
          storageConnectorApi.getOnlineStorageConnector(leftFeatureGroup.getFeatureStore());
      return SparkEngine.getInstance().jdbc(onlineConnector, fsQuery.getStorageQuery(Storage.ONLINE));
    } else {
      registerOnDemandFeatureGroups(fsQuery.getOnDemandFeatureGroups());
      registerHudiFeatureGroups(fsQuery.getHudiCachedFeatureGroups(), readOptions);

      LOGGER.info("Executing query: " + fsQuery.getStorageQuery(Storage.OFFLINE));
      return SparkEngine.getInstance().sql(fsQuery.getStorageQuery(Storage.OFFLINE));
    }
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    show(false, numRows);
  }

  public void show(boolean online, int numRows) throws FeatureStoreException, IOException {
    read(online).show(numRows);
  }

  public String toString() {
    return toString(Storage.OFFLINE);
  }

  public String toString(Storage storage) {
    try {
      return queryConstructorApi
          .constructQuery(leftFeatureGroup.getFeatureStore(), this)
          .getStorageQuery(storage);
    } catch (FeatureStoreException | IOException e) {
      return e.getMessage();
    }
  }

  private void registerOnDemandFeatureGroups(List<OnDemandFeatureGroupAlias> onDemandFeatureGroups)
      throws FeatureStoreException {
    if (onDemandFeatureGroups == null || onDemandFeatureGroups.isEmpty()) {
      return;
    }

    for (OnDemandFeatureGroupAlias onDemandFeatureGroupAlias : onDemandFeatureGroups) {
      String alias = onDemandFeatureGroupAlias.getAlias();
      OnDemandFeatureGroup onDemandFeatureGroup = onDemandFeatureGroupAlias.getOnDemandFeatureGroup();

      SparkEngine.getInstance().registerOnDemandTemporaryTable(onDemandFeatureGroup.getQuery(),
          onDemandFeatureGroup.getStorageConnector(), alias);
    }
  }

  private void registerHudiFeatureGroups(List<HudiFeatureGroupAlias> hudiFeatureGroups,
                                         Map<String, String> readOptions) {
    for (HudiFeatureGroupAlias hudiFeatureGroupAlias : hudiFeatureGroups) {
      String alias = hudiFeatureGroupAlias.getAlias();
      FeatureGroup featureGroup = hudiFeatureGroupAlias.getFeatureGroup();

      SparkEngine.getInstance().registerHudiTemporaryTable(featureGroup, alias,
          hudiFeatureGroupAlias.getLeftFeatureGroupStartTimestamp(),
          hudiFeatureGroupAlias.getLeftFeatureGroupEndTimestamp(),
          readOptions);
    }
  }
}
