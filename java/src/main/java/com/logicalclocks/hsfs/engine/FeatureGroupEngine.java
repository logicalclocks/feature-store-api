package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
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
  public void createFeatureGroup(FeatureGroup featureGroup, Dataset<Row> dataset,
                                 List<String> primaryKeys, List<String> partitionKeys, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureGroup.setFeatures(utils.parseSchema(dataset));

    LOGGER.info("Featuregroup features: " + featureGroup.getFeatures());

    /* set primary features */
    if (primaryKeys != null) {
      primaryKeys.forEach(pk ->
          featureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPrimary(true);
            }
          }));
    }

    /* set partition key features */
    if (partitionKeys != null) {
      partitionKeys.forEach(pk ->
          featureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPartition(true);
            }
          }));
    }

    // Send Hopsworks the request to create a new feature group
    featureGroupApi.create(featureGroup);

    // Write the dataframe
    saveDataframe(featureGroup, dataset, SaveMode.Append, writeOptions);
  }

  public Dataset<Row> read(FeatureGroup featureGroup, Storage storage) throws FeatureStoreException {
    switch (storage) {
      case OFFLINE:
        return readOfflineFeatureGroup(featureGroup);
      case ONLINE:
        return readOnlineFeatureGroup(featureGroup);
      default:
        throw new FeatureStoreException("ALL storage not supported when reading a feature group");
    }
  }

  private Dataset<Row> readOfflineFeatureGroup(FeatureGroup offlineFeatureGroup) {
    String tableName = utils.getTableName(offlineFeatureGroup);
    // TODO(Fabio) here we should probably use the dataframe API to integrate better HUDI
    return SparkEngine.getInstance().sql("SELECT * FROM " + tableName);
  }

  private Dataset<Row> readOnlineFeatureGroup(FeatureGroup onlineFeatureGroup) {
    // TODO(Fabio): Fix this
    return null;
  }

  public void saveDataframe(FeatureGroup offlineFeatureGroup, Dataset<Row> dataset,
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
