package com.logicalclocks.featurestore;

import com.logicalclocks.featurestore.engine.SparkEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainClass {

  private final static Logger LOGGER = LoggerFactory.getLogger(MainClass.class);

  public static void main(String[] args) throws Exception {

    HopsworksConnection connection = HopsworksConnection.builder().build();

    FeatureStore fs = connection.getFeatureStore();
    LOGGER.info("Feature Store " + fs);

    FeatureGroup fg = fs.getFeatureGroup("attendances_features", 1);

    LOGGER.info("Name " + fg.getName());

    LOGGER.info("Brace yourself, I'm running the query");
    fg.read().show(10);
    SparkEngine.getInstance().getSparkSession().close();
  }
}
