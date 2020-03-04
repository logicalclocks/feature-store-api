package com.logicalclocks.featurestore.engine;

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
import java.util.HashMap;
import java.util.Map;

public class TrainingDatasetEngine {

  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();
  private Utils utils = new Utils();

  //TODO:
  //      Parse schema
  //      Register training dataset
  //      Write dataset
  //      Compute statistics
  public void  saveTrainingDataset(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    if (trainingDataset.getFeaturesQuery() != null) {
      // Compile the query and get the dataframe
      trainingDataset.setFeaturesDataframe(trainingDataset.getFeaturesQuery().read());
    }

    // TODO(Fabio): make sure we can implement the serving part as well
    trainingDataset.setFeatures(utils.parseSchema(trainingDataset.getFeaturesDataframe()));

    // Make the rest call to create the training dataset metadata
    TrainingDataset apiTD = trainingDatasetApi.createTrainingDataset(trainingDataset);
    // Update the original object - Hopsworks returns the full location
    trainingDataset.setLocation(apiTD.getLocation());

    // Build write options map
    Map<String, String> writeOptions =
        getWriteOptions(trainingDataset.getWriteOptions(), trainingDataset.getDataFormat());

    write(trainingDataset, trainingDataset.getFeaturesDataframe(), writeOptions, SaveMode.Overwrite);
  }

  public void insert(TrainingDataset trainingDataset, Dataset<Row> dataset,
                     Map<String, String> providedOptions, SaveMode saveMode)
      throws FeatureStoreException {
    // validate that the schema matches
    utils.schemaMatches(dataset, trainingDataset.getFeatures());

    Map<String, String> writeOptions =
        getWriteOptions(providedOptions, trainingDataset.getDataFormat());

    write(trainingDataset, dataset, writeOptions, saveMode);
  }

  public Dataset<Row> read(TrainingDataset trainingDataset, Map<String, String> providedOptions) {
    String path = getDataLocation(trainingDataset);
    Map<String, String> readOptions = getReadOptions(providedOptions, trainingDataset.getDataFormat());
    return read(trainingDataset.getDataFormat(), readOptions, path);
  }

  private void write(TrainingDataset trainingDataset, Dataset<Row> dataset,
                     Map<String, String> writeOptions, SaveMode saveMode) {
    String path = getDataLocation(trainingDataset);

    if (trainingDataset.getSplits() == null) {
      // Write a single dataset
      writeSingle(dataset, trainingDataset.getDataFormat(),
          writeOptions, saveMode, path);
    } else {
      writeSplits(dataset.randomSplit(trainingDataset.getSplits()),
          trainingDataset.getDataFormat(), writeOptions, saveMode, path);
    }
  }

  // The actual data will be stored in training_ds_version/training_ds the double directory is needed
  // for cases such as tfrecords in which we need to store also the schema
  // TODO(Fabio): is this something we want to keep doing?
  private String getDataLocation(TrainingDataset trainingDataset) {
    return Paths.get(trainingDataset.getLocation(), trainingDataset.getName()).toString();
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

  private void writeSplits(Dataset<Row>[] datasets, DataFormat dataFormat, Map<String, String> writeOptions,
                           SaveMode saveMode, String basePath) {
    for (int i=0; i < datasets.length; i++) {
      writeSingle(datasets[i], dataFormat, writeOptions, saveMode, basePath + Constants.SPLIT_SUFFIX + i);
    }
  }

  private void writeSingle(Dataset<Row> dataset, DataFormat dataFormat,
                           Map<String, String> writeOptions, SaveMode saveMode, String path) {
    dataset
        .write()
        .format(dataFormat.toString())
        .options(writeOptions)
        .mode(saveMode)
        .save(path);
  }

  private Dataset<Row> read(DataFormat dataFormat, Map<String, String> readOptions, String path) {
    return SparkEngine.getInstance().getSparkSession()
        .read()
        .format(dataFormat.toString())
        .options(readOptions)
        .load(path);
  }
}
