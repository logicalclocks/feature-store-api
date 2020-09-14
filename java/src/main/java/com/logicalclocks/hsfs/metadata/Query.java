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
import com.logicalclocks.hsfs.JoinType;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.engine.SparkEngine;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Query {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Getter @Setter
  private FeatureGroup leftFeatureGroup;
  @Getter @Setter
  private List<Feature> leftFeatures;

  @Getter @Setter
  private List<Join> joins = new ArrayList<>();

  private QueryConstructorApi queryConstructorApi;
  private StorageConnectorApi storageConnectorApi;

  public Query(FeatureGroup leftFeatureGroup, List<Feature> leftFeatures) {
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

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(Storage.OFFLINE, null, null);
  }

  public Dataset<Row> read(Storage storage, String startTime, String  endTime)
          throws FeatureStoreException, IOException {
    if (storage == null) {
      throw new FeatureStoreException("Storage not supported");
    }

    String sqlQuery =
        queryConstructorApi.constructQuery(leftFeatureGroup.getFeatureStore(), this).getStorageQuery(storage);
    LOGGER.info("Executing query: " + sqlQuery);

    switch (storage) {
      case OFFLINE:
        if (leftFeatureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI) {
          return SparkEngine.getInstance().sql(sqlQuery, leftFeatureGroup, startTime, endTime);
        } else {
          return SparkEngine.getInstance().sql(sqlQuery);
        }
      case ONLINE:
        StorageConnector onlineConnector
            = storageConnectorApi.getOnlineStorageConnector(leftFeatureGroup.getFeatureStore());
        return SparkEngine.getInstance().jdbc(onlineConnector, sqlQuery);
      default:
        throw new FeatureStoreException("Storage not supported");
    }
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    show(Storage.OFFLINE, numRows);
  }

  public void show(Storage storage, int numRows) throws FeatureStoreException, IOException {
    read(storage, null, null).show(numRows);
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
}
