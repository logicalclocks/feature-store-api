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
import com.logicalclocks.base.constructor.QueryBase;
import com.logicalclocks.base.FeatureGroupBase;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.engine.SparkEngine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Query extends QueryBase<Query> {

  public Query(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    this.leftFeatureGroup = leftFeatureGroup;
    this.leftFeatures = addFeatureGroupToFeatures(leftFeatureGroup, leftFeatures);
  }

  public String sql() {
    return sql(Storage.OFFLINE, FsQuery.class);
  }

  public String sql(Storage storage) {
    return sql(storage, FsQuery.class);
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
