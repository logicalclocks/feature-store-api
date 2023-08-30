/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark.constructor;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.spark.StreamFeatureGroup;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;

import com.logicalclocks.hsfs.spark.util.StorageConnectorUtils;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class Query extends QueryBase<Query, StreamFeatureGroup, Dataset<Row>> {

  private final StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();

  public Query(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    super(leftFeatureGroup, leftFeatures);
  }

  @Override
  public String sql() {
    return sql(Storage.OFFLINE, FsQuery.class);
  }

  @Override
  public String sql(Storage storage) {
    return sql(storage, FsQuery.class);
  }

  @Override
  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  @Override
  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return read(online, null);
  }

  @Override
  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    FsQuery fsQuery = (FsQuery)
        queryConstructorApi.constructQuery(leftFeatureGroup.getFeatureStore(), this, FsQuery.class);

    if (online) {
      QueryBase.LOGGER.info("Executing query: " + fsQuery.getStorageQuery(Storage.ONLINE));
      StorageConnector.JdbcConnector onlineConnector =
          storageConnectorApi.getOnlineStorageConnector(
              leftFeatureGroup.getFeatureStore(), StorageConnector.JdbcConnector.class);
      return storageConnectorUtils.read(onlineConnector, fsQuery.getStorageQuery(Storage.ONLINE));
    } else {
      fsQuery.registerOnDemandFeatureGroups();
      fsQuery.registerHudiFeatureGroups(readOptions);

      QueryBase.LOGGER.info("Executing query: " + fsQuery.getStorageQuery(Storage.OFFLINE));
      return SparkEngine.getInstance().sql(fsQuery.getStorageQuery(Storage.OFFLINE));
    }
  }

  @Override
  public void show(int numRows) throws FeatureStoreException, IOException {
    show(false, numRows);
  }

  @Override
  public void show(boolean online, int numRows) throws FeatureStoreException, IOException {
    SparkEngine.getInstance().objectToDataset(read(online)).show(numRows);
  }
}
