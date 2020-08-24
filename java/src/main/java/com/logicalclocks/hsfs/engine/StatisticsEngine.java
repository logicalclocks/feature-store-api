package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.Statistics;
import com.logicalclocks.hsfs.metadata.StatisticsApi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StatisticsEngine {

  private StatisticsApi statisticsApi;

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsEngine.class);

  public StatisticsEngine(EntityEndpointType entityType) {
    this.statisticsApi = new StatisticsApi(entityType);
  }

  public Statistics computeStatistics(FeatureGroup featureGroup, Dataset<Row> dataFrame)
      throws FeatureStoreException, IOException {
    String commitTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

    String content = SparkEngine.getInstance().profile(dataFrame, featureGroup.getStatisticColumns(),
        featureGroup.getHistograms(), featureGroup.getCorrelations());
    return statisticsApi.post(featureGroup, new Statistics(commitTime, content));
  }

}
