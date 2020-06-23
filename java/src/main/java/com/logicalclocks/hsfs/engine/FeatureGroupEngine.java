package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureGroupEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private Utils utils = new Utils();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);

  //TODO:
  //      Compute statistics

  /**
   * Create the metadata and write the data to the online/offline feature store
   * @param featureGroup
   * @param dataset
   * @param primaryKeys
   * @param partitionKeys
   * @param storage
   * @param writeOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void saveFeatureGroup(FeatureGroup featureGroup, Dataset<Row> dataset,
                               List<String> primaryKeys, List<String> partitionKeys,
                               Storage storage, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {

    if (featureGroup.getFeatureStore() != null) {
      featureGroup.setFeatures(utils.parseSchema(dataset));
    }

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
    featureGroupApi.save(featureGroup);

    // Write the dataframe
    saveDataframe(featureGroup, dataset, storage, SaveMode.Append, writeOptions);
  }

  public void saveDataframe(FeatureGroup featureGroup, Dataset<Row> dataset, Storage storage,
                            SaveMode saveMode, Map<String, String> writeOptions)
      throws IOException, FeatureStoreException {
    switch (storage) {
      case OFFLINE:
        saveOfflineDataframe(featureGroup, dataset, saveMode, writeOptions);
        break;
      case ONLINE:
        saveOnlineDataframe(featureGroup, dataset, saveMode, writeOptions);
        break;
      case ALL:
        saveOfflineDataframe(featureGroup, dataset, saveMode, writeOptions);
        saveOnlineDataframe(featureGroup, dataset, saveMode, writeOptions);
    }
  }

  /**
   * Write dataframe to the offline feature store
   * @param featureGroup
   * @param dataset
   * @param saveMode
   * @param writeOptions
   */
  private void saveOfflineDataframe(FeatureGroup featureGroup, Dataset<Row> dataset,
                            SaveMode saveMode, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException{

    if (saveMode == SaveMode.Overwrite) {
      // If we set overwrite, then the directory will be removed and with it all the metadata
      // related to the feature group will be lost. We need to keep them.
      // So we call Hopsworks to manage to truncate the table and re-create the metadata
      // After that it's going to be just a normal append
      featureGroupApi.deleteContent(featureGroup);
      saveMode = SaveMode.Append;
    }

    dataset
        .write()
        .format(Constants.HIVE_FORMAT)
        .mode(saveMode)
        // write options cannot be null
        .options(writeOptions == null ? new HashMap<>() : writeOptions)
        .partitionBy(utils.getPartitionColumns(featureGroup))
        .saveAsTable(utils.getTableName(featureGroup));
  }

  private void saveOnlineDataframe(FeatureGroup featureGroup, Dataset<Row> dataset,
                                   SaveMode saveMode, Map<String, String> providedWriteOptions)
      throws IOException, FeatureStoreException {
    StorageConnector storageConnector = storageConnectorApi.getOnlineStorageConnector(featureGroup.getFeatureStore());
    Map<String, String> writeOptions = getOnlineOptions(providedWriteOptions, featureGroup, storageConnector);
    writeOnlineDataframe(dataset, saveMode, writeOptions);
  }

  /**
   * Build the option maps to write the dataset to the JDBC sink. URL, username and password are taken from the
   * storage connector.
   * They can however be overwritten by the user if they pass a option map. For instance if they want to change the
   * @param providedWriteOptions
   * @param featureGroup
   * @param storageConnector
   * @return
   * @throws FeatureStoreException
   */
  private Map<String, String> getOnlineOptions(Map<String, String> providedWriteOptions,
                                               FeatureGroup featureGroup,
                                               StorageConnector storageConnector) throws FeatureStoreException {
    Map<String, String> writeOptions = storageConnector.getSparkOptions();
    writeOptions.put(Constants.JDBC_TABLE, utils.getFgName(featureGroup));

    // add user provided configuration
    if (providedWriteOptions != null) {
      writeOptions.putAll(providedWriteOptions);
    }

    return writeOptions;
  }

  /**
   * Write dataset on the JDBC sink
   * @param dataset
   * @param saveMode
   * @param writeOptions
   * @throws FeatureStoreException
   */
  private void writeOnlineDataframe(Dataset<Row> dataset, SaveMode saveMode, Map<String, String> writeOptions) {
    dataset
        .write()
        .format(Constants.JDBC_FORMAT)
        .options(writeOptions)
        .mode(saveMode)
        .save();
  }

  public void delete(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    featureGroupApi.delete(featureGroup);
  }

  public void addTag(FeatureGroup featureGroup, String name, String value) throws FeatureStoreException, IOException {
    featureGroupApi.addTag(featureGroup, name, value);
  }

  public Map<String, String> getTags(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    return featureGroupApi.getTags(featureGroup);
  }

  public String getTag(FeatureGroup featureGroup, String name) throws FeatureStoreException, IOException {
    return featureGroupApi.getTag(featureGroup, name);
  }

  public void deleteTag(FeatureGroup featureGroup, String name) throws FeatureStoreException, IOException {
    featureGroupApi.deleteTag(featureGroup, name);
  }

}
