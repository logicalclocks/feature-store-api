package com.logicalclocks.featurestore;

import com.logicalclocks.featurestore.engine.SparkEngine;
import com.logicalclocks.featurestore.metadata.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    StorageConnector hopsFSConnector = fs.getStorageConnector("demo_featurestore_admin000_Training_Datasets",
        StorageConnectorType.HOPSFS);

    LOGGER.info("Storage connector ID" + hopsFSConnector.getId());

    TrainingDataset td = TrainingDataset.builder()
        .name("new_api_td")
        .description("This is a test")
        .version(1)
        .features(query)
        .dataFormat(DataFormat.CSV)
        .storageConnector(hopsFSConnector)
        .build();

    fs.createTrainingDataset(td);

    SparkEngine.getInstance().getSparkSession().close();
  }
}
