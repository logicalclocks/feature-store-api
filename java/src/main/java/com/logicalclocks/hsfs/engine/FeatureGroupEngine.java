package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.OfflineFeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FeatureGroupEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private Utils utils = new Utils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);

  //TODO:
  //      Compute statistics
  public void createFeatureGroup(OfflineFeatureGroup offlineFeatureGroup, Dataset<Row> dataset,
                                 List<String> primaryKeys, List<String> partitionKeys, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    offlineFeatureGroup.setFeatures(utils.parseSchema(dataset));

    LOGGER.info("Featuregroup features: " + offlineFeatureGroup.getFeatures());

    /* set primary features */
    if (primaryKeys != null) {
      primaryKeys.forEach(pk ->
          offlineFeatureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPrimary(true);
            }
          }));
    }

    /* set partition key features */
    if (partitionKeys != null) {
      partitionKeys.forEach(pk ->
          offlineFeatureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPartition(true);
            }
          }));
    }

    // Send Hopsworks the request to create a new feature group
    featureGroupApi.create(offlineFeatureGroup);

    // Write the dataframe
    saveDataframe(offlineFeatureGroup, dataset, SaveMode.Append, writeOptions);
  }

  public Dataset<Row> read(FeatureGroup featureGroup) {
    if (featureGroup instanceof OfflineFeatureGroup) {
      return readOfflineFeatureGroup((OfflineFeatureGroup) featureGroup);
    }
    return null;
  }

  private Dataset<Row> readOfflineFeatureGroup(OfflineFeatureGroup offlineFeatureGroup) {
    String tableName = utils.getTableName(offlineFeatureGroup);
    return SparkEngine.getInstance().sql("SELECT * FROM " + tableName);
  }

  public void saveDataframe(OfflineFeatureGroup offlineFeatureGroup, Dataset<Row> dataset,
                            SaveMode saveMode, Map<String, String> writeOptions) {
    dataset
        .write()
        .format(Constants.HIVE_FORMAT)
        .mode(saveMode)
        .options(writeOptions)
        .partitionBy(utils.getPartitionColumns(offlineFeatureGroup))
        .saveAsTable(utils.getTableName(offlineFeatureGroup));
  }
}
