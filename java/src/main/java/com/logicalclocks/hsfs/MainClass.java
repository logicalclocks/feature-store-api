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

package com.logicalclocks.hsfs;

import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.constructor.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainClass {

  private static final Logger LOGGER = LoggerFactory.getLogger(MainClass.class);

  public static void main(String[] args) throws Exception {

    HopsworksConnection connection = HopsworksConnection.builder().build();

    FeatureStore fs = connection.getFeatureStore();
    LOGGER.info("Feature Store " + fs);

    FeatureGroup housingFeatureGroup = fs.createFeatureGroup()
        .name("housing")
        .description("House pricing model features")
        .version(1)
        .primaryKeys(Arrays.asList("house_id", "date"))
        .partitionKeys(Arrays.asList("country"))
        .onlineEnabled(true)
        .build();

    FeatureGroup attendance = fs.getFeatureGroup("attendances_features", 1);
    FeatureGroup players = fs.getFeatureGroup("players_features", 1);

    Query query = attendance.selectAll().join(players.selectAll());

    // Get the storage connector where to write
    //StorageConnector hopsFSConnector = fs.getStorageConnector("demo_featurestore_admin000_Training_Datasets",
    //    StorageConnectorType.HOPSFS);

    //LOGGER.info("Storage connector ID" + hopsFSConnector.getId());
    List<Split> splits = new ArrayList<>();
    splits.add(new Split("test", 0.8f));

    TrainingDataset td = fs.createTrainingDataset()
        .name("new_api_td")
        .description("This is a test")
        .version(1)
        .dataFormat(DataFormat.CSV)
        .splits(splits)
        .build();

    td.save(query);

    SparkEngine.getInstance().getSparkSession().close();
  }
}
