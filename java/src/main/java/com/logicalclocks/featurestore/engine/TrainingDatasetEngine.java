package com.logicalclocks.featurestore.engine;

import com.logicalclocks.featurestore.DataFormat;
import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.TrainingDataset;
import com.logicalclocks.featurestore.metadata.TrainingDatasetApi;
import com.logicalclocks.featurestore.util.Constants;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TrainingDatasetEngine {

  private TrainingDatasetApi trainingDatasetApi = new TrainingDatasetApi();

  //TODO:
  //      Parse schema
  //      Register training dataset
  //      Write dataset
  //      Compute statistics
  public void  saveTrainingDataset(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    if (trainingDataset.getFeatures() != null) {
      // Compile the query and get the dataframe
      trainingDataset.setFeaturesDataframe(trainingDataset.getFeatures().read());
    }

    // Make the rest call to create the training dataset metadata
    TrainingDataset apiTD = trainingDatasetApi.createTrainingDataset(trainingDataset);

    // Build write options map
    Map<String, String> writeOptions =
        getWriteOptions(trainingDataset.getWriteOptions(), trainingDataset.getDataFormat());

    // The actual data will be stored in training_ds_version/training_ds the double directory is needed
    // for cases such as tfrecords in which we need to store also the schema
    String path = Paths.get(apiTD.getHdfsStorePath(), trainingDataset.getName()).toString();

    // Write the dataframe
    trainingDataset.getFeaturesDataframe()
        .write()
        .format(trainingDataset.getDataFormat().toString())
        .options(writeOptions)
        .mode(SaveMode.Overwrite)
        .save(path);
  }

  private Map<String, String> getWriteOptions(Map<String, String> providedOptions, DataFormat dataformat) {
    Map<String, String> writeOptions = new HashMap<>();
    switch (dataformat) {
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

}
