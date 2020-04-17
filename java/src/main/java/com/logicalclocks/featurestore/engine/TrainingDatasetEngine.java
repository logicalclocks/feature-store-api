package com.logicalclocks.featurestore.engine;

import com.google.common.base.Strings;
import com.logicalclocks.featurestore.DataFormat;
import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.TrainingDataset;
import com.logicalclocks.featurestore.metadata.TrainingDatasetApi;
import com.logicalclocks.featurestore.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrainingDatasetEngine {

  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();
  private Utils utils = new Utils();

  //TODO:
  //      Compute statistics

  /**
   * Make a REST call to Hopsworks to create the metadata and write the data on the File System
   * @param trainingDataset
   * @param dataset
   * @param userWriteOptions
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void create(TrainingDataset trainingDataset, Dataset<Row> dataset,
                     Map<String, String> userWriteOptions)
      throws FeatureStoreException, IOException {
    // TODO(Fabio): make sure we can implement the serving part as well
    trainingDataset.setFeatures(utils.parseSchema(dataset));

    // Make the rest call to create the training dataset metadata
    TrainingDataset apiTD = trainingDatasetApi.createTrainingDataset(trainingDataset);
    // Update the original object - Hopsworks returns the full location
    trainingDataset.setLocation(apiTD.getLocation());

    // Build write options map
    Map<String, String> writeOptions =
        getWriteOptions(userWriteOptions, trainingDataset.getDataFormat());

    write(trainingDataset, dataset, writeOptions, SaveMode.Overwrite);
  }

  /**
   * Insert (append or overwrite) data on a training dataset
   * @param trainingDataset
   * @param dataset
   * @param providedOptions
   * @param saveMode
   * @throws FeatureStoreException
   */
  public void insert(TrainingDataset trainingDataset, Dataset<Row> dataset,
                     Map<String, String> providedOptions, SaveMode saveMode)
      throws FeatureStoreException {
    // validate that the schema matches
    utils.schemaMatches(dataset, trainingDataset.getFeatures());

    Map<String, String> writeOptions =
        getWriteOptions(providedOptions, trainingDataset.getDataFormat());

    write(trainingDataset, dataset, writeOptions, saveMode);
  }

  /**
   * Setup Spark to write the data on the File System
   * @param trainingDataset
   * @param dataset
   * @param writeOptions
   * @param saveMode
   */
  private void write(TrainingDataset trainingDataset, Dataset<Row> dataset,
                     Map<String, String> writeOptions, SaveMode saveMode) {

    if (trainingDataset.getStorageConnector() != null) {
      SparkEngine.getInstance().configureConnector(trainingDataset.getStorageConnector());
    }

    if (trainingDataset.getSplits() == null) {
      // Write a single dataset

      // The actual data will be stored in training_ds_version/training_ds the double directory is needed
      // for cases such as tfrecords in which we need to store also the schema
      // also in case of multiple splits, the single splits will be stored inside the training dataset dir
      String path = Paths.get(trainingDataset.getLocation(), trainingDataset.getName()).toString();

      writeSingle(dataset, trainingDataset.getDataFormat(),
          writeOptions, saveMode, path);
    } else {
      // Make sure the names and factors are ordered
      List<String> splitNames = new ArrayList<>();
      List<Double> splitFactors = new ArrayList<>();
      for (Map.Entry<String, Double> entry : trainingDataset.getSplits().entrySet()) {
        splitNames.add(entry.getKey());
        splitFactors.add(entry.getValue());
      }

      // The actual data will be stored in training_ds_version/split_name
      Dataset<Row>[] datasetSplits = null;
      if (trainingDataset.getSeed() != null) {
        datasetSplits = dataset.randomSplit(
            splitFactors.stream().mapToDouble(Double::doubleValue).toArray(), trainingDataset.getSeed());
      } else {
        datasetSplits = dataset.randomSplit(splitFactors.stream().mapToDouble(Double::doubleValue).toArray());
      }

      writeSplits(datasetSplits,
          trainingDataset.getDataFormat(), writeOptions, saveMode,
          trainingDataset.getLocation(), splitNames);
    }
  }

  public Dataset<Row> read(TrainingDataset trainingDataset, String split, Map<String, String> providedOptions) {
    if (trainingDataset.getStorageConnector() != null) {
      SparkEngine.getInstance().configureConnector(trainingDataset.getStorageConnector());
    }

    String path = "";
    if (Strings.isNullOrEmpty(split)) {
      // ** glob means "all sub directories"
      // TODO(Fabio): make sure it works on S3
      path = Paths.get(trainingDataset.getLocation(), "**").toString();
    } else {
      path = Paths.get(trainingDataset.getLocation(), split).toString();
    }

    Map<String, String> readOptions = getReadOptions(providedOptions, trainingDataset.getDataFormat());
    return read(trainingDataset.getDataFormat(), readOptions, path);
  }

  private Map<String, String> getWriteOptions(Map<String, String> providedOptions, DataFormat dataFormat) {
    Map<String, String> writeOptions = new HashMap<>();
    switch (dataFormat) {
      case CSV:
        writeOptions.put(Constants.HEADER, "true");
        writeOptions.put(Constants.DELIMITER, ",");
        break;
      case TSV:
        writeOptions.put(Constants.HEADER, "true");
        writeOptions.put(Constants.DELIMITER, "\t");
        break;
      case TFRECORDS:
        writeOptions.put(Constants.TF_CONNECTOR_RECORD_TYPE, "Example");
    }

    if (providedOptions != null && !providedOptions.isEmpty()) {
      writeOptions.putAll(providedOptions);
    }

    return writeOptions;
  }

  private Map<String, String> getReadOptions(Map<String, String> providedOptions, DataFormat dataFormat) {
    Map<String, String> readOptions = new HashMap<>();
    switch (dataFormat) {
      case CSV:
        readOptions.put(Constants.HEADER, "true");
        readOptions.put(Constants.DELIMITER, ",");
        readOptions.put(Constants.INFER_SCHEMA, "true");
        break;
      case TSV:
        readOptions.put(Constants.HEADER, "true");
        readOptions.put(Constants.DELIMITER, "\t");
        readOptions.put(Constants.INFER_SCHEMA, "true");
        break;
      case TFRECORDS:
        readOptions.put(Constants.TF_CONNECTOR_RECORD_TYPE, "Example");
    }

    if (providedOptions != null && !providedOptions.isEmpty()) {
      readOptions.putAll(providedOptions);
    }

    return readOptions;
  }

  /**
   * Write multiple training dataset splits and name them.
   * @param datasets
   * @param dataFormat
   * @param writeOptions
   * @param saveMode
   * @param basePath
   * @param splitNames
   */
  private void writeSplits(Dataset<Row>[] datasets, DataFormat dataFormat, Map<String, String> writeOptions,
                           SaveMode saveMode, String basePath, List<String> splitNames) {
    for (int i=0; i < datasets.length; i++) {
      writeSingle(datasets[i], dataFormat, writeOptions, saveMode,
          Paths.get(basePath, splitNames.get(i)).toString());
    }
  }

  /**
   * Write a single dataset split
   * @param dataset
   * @param dataFormat
   * @param writeOptions
   * @param saveMode
   * @param path: it should be the full path
   */
  private void writeSingle(Dataset<Row> dataset, DataFormat dataFormat,
                           Map<String, String> writeOptions, SaveMode saveMode, String path) {
    dataset
        .write()
        .format(dataFormat.toString())
        .options(writeOptions)
        .mode(saveMode)
        .save(SparkEngine.sparkPath(path));
  }

  private Dataset<Row> read(DataFormat dataFormat, Map<String, String> readOptions, String path) {
    return SparkEngine.getInstance().getSparkSession()
        .read()
        .format(dataFormat.toString())
        .options(readOptions)
        .load(SparkEngine.sparkPath(path));
  }
}
