package com.logicalclocks.featurestore;

import com.logicalclocks.featurestore.engine.SparkEngine;
import com.logicalclocks.featurestore.metadata.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MainClass {

  private final static Logger LOGGER = LoggerFactory.getLogger(MainClass.class);

  public static void main(String[] args) throws Exception {

    HopsworksConnection connection = HopsworksConnection.builder().build();

    FeatureStore fs = connection.getFeatureStore();
    LOGGER.info("Feature Store " + fs);

    FeatureGroup attendance = fs.getFeatureGroup("attendances_features", 1);
    FeatureGroup players = fs.getFeatureGroup("players_features", 1);

    Query query = attendance.selectAll().join(players.selectAll());

    // Get the storage connector where to write
    //StorageConnector hopsFSConnector = fs.getStorageConnector("demo_featurestore_admin000_Training_Datasets",
    //    StorageConnectorType.HOPSFS);

    //LOGGER.info("Storage connector ID" + hopsFSConnector.getId());
    Map<String, Double> splits = new HashMap<>();
    splits.put("training", 0.70);
    splits.put("testing", 0.20);
    splits.put("validatrion", 0.10);

    TrainingDataset td = fs.createTrainingDataset()
        .name("new_api_td")
        .description("This is a test")
        .version(1)
        .dataFormat(DataFormat.CSV)
        .splits(splits)
        .build();

    td.create(query);

    SparkEngine.getInstance().getSparkSession().close();
  }
}
