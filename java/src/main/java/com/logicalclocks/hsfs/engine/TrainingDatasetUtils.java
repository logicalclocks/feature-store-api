package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.TrainingDatasetType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TrainingDatasetUtils {


  public List<TrainingDatasetFeature> parseTrainingDatasetSchema(Dataset<Row> dataset) throws FeatureStoreException {
    List<TrainingDatasetFeature> features = new ArrayList<>();

    int index = 0;
    for (StructField structField : dataset.schema().fields()) {
      // TODO(Fabio): unit test this one for complext types
      features.add(new TrainingDatasetFeature(
          structField.name().toLowerCase(), structField.dataType().catalogString(), index++));
    }

    return features;
  }

  public void trainingDatasetSchemaMatch(Dataset<Row> dataset, List<TrainingDatasetFeature> features)
      throws FeatureStoreException {
    StructType tdStructType = new StructType(features.stream()
        .sorted(Comparator.comparingInt(TrainingDatasetFeature::getIndex))
        .map(f -> new StructField(f.getName(),
            // What should we do about the nullables
            new CatalystSqlParser().parseDataType(f.getType()), true, Metadata.empty())
        ).toArray(StructField[]::new));

    if (!dataset.schema().equals(tdStructType)) {
      throw new FeatureStoreException("The Dataframe schema: " + dataset.schema()
          + " does not match the training dataset schema: " + tdStructType);
    }
  }

  public TrainingDatasetType getTrainingDatasetType(StorageConnector storageConnector) {
    if (storageConnector == null) {
      return TrainingDatasetType.HOPSFS_TRAINING_DATASET;
    } else if (storageConnector.getStorageConnectorType() == StorageConnectorType.HOPSFS) {
      return TrainingDatasetType.HOPSFS_TRAINING_DATASET;
    } else {
      return TrainingDatasetType.EXTERNAL_TRAINING_DATASET;
    }
  }

}
