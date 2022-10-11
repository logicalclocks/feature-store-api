/*
 *  Copyright (c) 2022. Logical Clocks AB
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

import com.logicalclocks.hsfs.generic.Feature;
import com.logicalclocks.hsfs.generic.FeatureStoreException;
import com.logicalclocks.hsfs.generic.Storage;
import com.logicalclocks.hsfs.spark.StorageConnector;
import com.logicalclocks.hsfs.generic.constructor.FilterLogic;
import com.logicalclocks.hsfs.generic.constructor.FsQuery;
import com.logicalclocks.hsfs.generic.constructor.Join;
import com.logicalclocks.hsfs.generic.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.generic.metadata.QueryConstructorApi;
import com.logicalclocks.hsfs.generic.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
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

public class Query extends com.logicalclocks.hsfs.generic.constructor.Query {

  private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

  @Getter
  @Setter
  private FeatureGroupBase leftFeatureGroup;
  @Getter
  @Setter
  private List<Feature> leftFeatures;
  @Getter
  @Setter
  private Long leftFeatureGroupStartTime;
  @Getter
  @Setter
  private Long leftFeatureGroupEndTime;
  @Getter
  @Setter
  private List<Join> joins = new ArrayList<>();
  @Getter
  @Setter
  private FilterLogic filter;
  @Getter
  @Setter
  private Boolean hiveEngine = false;

  private QueryConstructorApi queryConstructorApi = new QueryConstructorApi();
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private FeatureGroupUtils utils = new FeatureGroupUtils();

  public Query(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    this.leftFeatureGroup = leftFeatureGroup;
    this.leftFeatures = leftFeatures;
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(false, null);
  }

  public Dataset<Row> read(boolean online) throws FeatureStoreException, IOException {
    return read(online, null);
  }

  @Override
  public Dataset<Row> read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    FsQuery fsQuery = queryConstructorApi.constructQuery(leftFeatureGroup.getFeatureStore(), this);

    if (online) {
      LOGGER.info("Executing query: " + fsQuery.getStorageQuery(Storage.ONLINE));
      StorageConnector.SparkJdbcConnector onlineConnector =
          (StorageConnector.SparkJdbcConnector) storageConnectorApi.getOnlineStorageConnector(leftFeatureGroup.getFeatureStore());
      return onlineConnector.read(fsQuery.getStorageQuery(Storage.ONLINE),null, null, null);
    } else {
      fsQuery.registerOnDemandFeatureGroups();
      fsQuery.registerHudiFeatureGroups(readOptions);

      LOGGER.info("Executing query: " + fsQuery.getStorageQuery(Storage.OFFLINE));
      return SparkEngine.getInstance().sql(fsQuery.getStorageQuery(Storage.OFFLINE));
    }
  }

  public void show(int numRows) {
  }

  @Override
  public void show(boolean online, int numRows) {
  }
}
