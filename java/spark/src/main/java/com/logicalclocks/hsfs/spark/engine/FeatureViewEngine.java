/*
 *  Copyright (c) 2022-2023. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.spark.engine;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.logicalclocks.hsfs.spark.constructor.Query;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.TrainingDatasetType;
import com.logicalclocks.hsfs.engine.FeatureViewEngineBase;
import com.logicalclocks.hsfs.metadata.Statistics;
import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.spark.FeatureView;
import com.logicalclocks.hsfs.spark.StreamFeatureGroup;
import com.logicalclocks.hsfs.spark.FeatureStore;
import com.logicalclocks.hsfs.spark.TrainingDataset;
import com.logicalclocks.hsfs.spark.TrainingDatasetBundle;

import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class FeatureViewEngine extends FeatureViewEngineBase<Query, FeatureView, FeatureStore, StreamFeatureGroup,
    Dataset<Row>> {

  private TrainingDatasetEngine trainingDatasetEngine = new TrainingDatasetEngine();
  private StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.TRAINING_DATASET);

  @Override
  public FeatureView update(FeatureView featureView) throws FeatureStoreException,
      IOException {
    featureViewApi.update(featureView, FeatureView.class);
    return featureView;
  }

  @Override
  public FeatureView get(FeatureStore featureStore, String name, Integer version)
      throws FeatureStoreException, IOException {
    FeatureView featureView = get(featureStore, name, version, FeatureView.class);
    featureView.setFeatureStore(featureStore);
    return featureView;
  }

  public TrainingDatasetBundle createTrainingDataset(
      FeatureView featureView, TrainingDataset trainingDataset, Map<String, String> userWriteOptions
  ) throws IOException, FeatureStoreException {
    setTrainSplit(trainingDataset);
    trainingDataset = createTrainingDataMetadata(featureView, trainingDataset);
    writeTrainingDataset(featureView, trainingDataset, userWriteOptions);
    return new TrainingDatasetBundle(trainingDataset.getVersion());
  }

  public void writeTrainingDataset(
      FeatureView featureView, TrainingDataset trainingDataset, Map<String, String> userWriteOptions
  ) throws IOException, FeatureStoreException {
    // Build write options map
    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(userWriteOptions, trainingDataset.getDataFormat());
    Query query = getBatchQuery(featureView, trainingDataset.getEventStartTime(),
        trainingDataset.getEventEndTime(), true, trainingDataset.getVersion(), Query.class);
    Dataset<Row>[] datasets = SparkEngine.getInstance().write(trainingDataset, query, Maps.newHashMap(),
        writeOptions, SaveMode.Overwrite);
    computeStatistics(featureView, trainingDataset, datasets);
  }

  public TrainingDatasetBundle getTrainingDataset(
      FeatureView featureView, Integer trainingDatasetVersion, List<String> requestedSplits,
      Map<String, String> userReadOptions
  ) throws IOException, FeatureStoreException, ParseException {
    TrainingDataset trainingDataset = featureView.getFeatureStore().createTrainingDataset()
        .name(featureView.getName())
        .version(trainingDatasetVersion)
        .build();
    return getTrainingDataset(featureView, trainingDataset, requestedSplits, userReadOptions);
  }

  public TrainingDatasetBundle getTrainingDataset(
      FeatureView featureView, TrainingDataset trainingDataset, Map<String, String> userReadOptions
  ) throws IOException, FeatureStoreException {
    return getTrainingDataset(featureView, trainingDataset, null, userReadOptions);
  }

  public TrainingDatasetBundle getTrainingDataset(
      FeatureView featureView, TrainingDataset trainingDataset, List<String> requestedSplits,
      Map<String, String> userReadOptions
  ) throws IOException, FeatureStoreException {
    TrainingDataset trainingDatasetUpdated = null;
    if (trainingDataset.getVersion() != null) {
      trainingDatasetUpdated = getTrainingDataMetadata(featureView, trainingDataset.getVersion());
    } else {
      trainingDatasetUpdated = createTrainingDataMetadata(featureView, trainingDataset);
    }
    if (requestedSplits != null) {
      int splitSize = trainingDatasetUpdated.getSplits().size();
      String methodName = "";
      if (splitSize != requestedSplits.size()) {
        if (splitSize == 0) {
          methodName = "getTrainingData";
        } else if (splitSize == 2) {
          methodName = "getTrainTestSplit";
        } else if (splitSize == 3) {
          methodName = "getTrainValidationTestSplit";
        }
        throw new FeatureStoreException(
            String.format("Incorrect `get` method is used. Use `FeatureView.%s` instead.", methodName));
      }
    }
    if (!TrainingDatasetType.IN_MEMORY_TRAINING_DATASET.equals(trainingDatasetUpdated.getTrainingDatasetType())) {
      try {
        List<Split> splits = trainingDatasetUpdated.getSplits();
        if (splits != null && !splits.isEmpty()) {
          Map<String, Dataset<Row>> datasets = Maps.newHashMap();
          for (Split split : splits) {
            datasets.put(split.getName(),
                castColumnType(
                    trainingDatasetUpdated.getDataFormat(),
                    trainingDatasetEngine.read(trainingDatasetUpdated, split.getName(), userReadOptions),
                    featureView.getFeatures()
                )
            );
          }
          return new TrainingDatasetBundle(trainingDatasetUpdated.getVersion(),
              datasets, featureView.getLabels());
        } else {
          return new TrainingDatasetBundle(trainingDatasetUpdated.getVersion(),
              castColumnType(
                  trainingDatasetUpdated.getDataFormat(),
                  trainingDatasetEngine.read(trainingDatasetUpdated, "", userReadOptions),
                  featureView.getFeatures()),
              featureView.getLabels()
          );
        }
      } catch (InvalidInputException e) {
        throw new IllegalStateException(
            "Failed to read datasets. Check if path exists or recreate a training dataset."
        );
      }
    } else {
      TrainingDatasetBundle trainingDatasetBundle;
      if (trainingDatasetUpdated.getSplits() != null && !trainingDatasetUpdated.getSplits().isEmpty()) {
        Query query = getBatchQuery(featureView, trainingDataset.getEventStartTime(), trainingDataset.getEventEndTime(),
            true, trainingDataset.getVersion(), Query.class);
        Dataset<Row>[] datasets = SparkEngine.getInstance().splitDataset(trainingDatasetUpdated, query,
            userReadOptions);
        trainingDatasetBundle = new TrainingDatasetBundle(trainingDatasetUpdated.getVersion(),
            convertSplitDatasetsToMap(trainingDatasetUpdated.getSplits(), datasets),
            featureView.getLabels());
        computeStatistics(featureView, trainingDatasetUpdated, datasets);
      } else {
        Dataset<Row> dataset = readDataset(featureView, trainingDatasetUpdated, userReadOptions);
        trainingDatasetBundle = new TrainingDatasetBundle(trainingDatasetUpdated.getVersion(), dataset,
            featureView.getLabels());
        computeStatistics(featureView, trainingDatasetUpdated, new Dataset[] {dataset});
      }
      return trainingDatasetBundle;
    }
  }

  private Dataset<Row> castColumnType(
      DataFormat dataFormat, Dataset<Row> dataset, List<TrainingDatasetFeature> features) throws FeatureStoreException {
    if (DataFormat.CSV.equals(dataFormat) || DataFormat.TSV.equals(dataFormat)) {
      return SparkEngine.getInstance().castColumnType(dataset, features);
    } else {
      return dataset;
    }
  }

  private void setTrainSplit(TrainingDataset trainingDataset) {
    if (trainingDataset.getSplits() != null
        && trainingDataset.getSplits().size() > 0
        && Strings.isNullOrEmpty(trainingDataset.getTrainSplit())) {
      LOGGER.info("Training dataset splits were defined but no `trainSplit` (the name of the split that is going to"
          + " be used for training) was provided. Setting this property to `train`.");
      trainingDataset.setTrainSplit("train");
    }
  }

  private TrainingDataset createTrainingDataMetadata(
      FeatureView featureView, TrainingDataset trainingDataset) throws IOException, FeatureStoreException {
    setEventTime(featureView, trainingDataset);
    return (TrainingDataset) featureViewApi.createTrainingData(
        featureView.getName(), featureView.getVersion(), trainingDataset, TrainingDataset.class);
  }

  private TrainingDataset getTrainingDataMetadata(
      FeatureView featureView, Integer trainingDatasetVersion) throws IOException, FeatureStoreException {
    return (TrainingDataset) featureViewApi.getTrainingData(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion(), trainingDatasetVersion, TrainingDataset.class);
  }

  public Statistics computeStatistics(FeatureView featureView, TrainingDataset trainingDataset,
                                      Dataset<Row>[] datasets)
      throws FeatureStoreException, IOException {
    if (trainingDataset.getStatisticsConfig().getEnabled()) {
      if (trainingDataset.getSplits() != null && !trainingDataset.getSplits().isEmpty()) {
        return statisticsEngine.computeAndSaveSplitStatistics(
            featureView, trainingDataset, convertSplitDatasetsToMap(trainingDataset.getSplits(), datasets));
      } else {
        return statisticsEngine.computeStatistics(featureView, trainingDataset, datasets[0]);
      }
    }
    return null;
  }

  protected Map<String, Dataset<Row>> convertSplitDatasetsToMap(List<Split> splits, Dataset<Row>[] datasets) {
    Map<String, Dataset<Row>> datasetSplits = Maps.newHashMap();
    for (int i = 0; i < datasets.length; i++) {
      datasetSplits.put(splits.get(i).getName(), datasets[i]);
    }
    return datasetSplits;
  }

  public void recreateTrainingDataset(FeatureView featureView, Integer version, Map<String, String> userWriteOptions)
      throws IOException, FeatureStoreException {
    TrainingDataset trainingDataset = getTrainingDataMetadata(featureView, version);
    writeTrainingDataset(featureView, trainingDataset, userWriteOptions);
  }

  private Dataset<Row> readDataset(FeatureView featureView, TrainingDataset trainingDataset,
                                   Map<String, String> userReadOptions) throws IOException,
      FeatureStoreException {
    Query query = getBatchQuery(featureView, trainingDataset.getEventStartTime(), trainingDataset.getEventEndTime(),
        true, trainingDataset.getVersion(), Query.class);
    return query.read(false, userReadOptions);
  }

  @Override
  public String getBatchQueryString(FeatureView featureView, Date startTime, Date endTime, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    Query query = getBatchQuery(featureView, startTime, endTime, false, trainingDataVersion, Query.class);
    return query.sql();
  }

  @Override
  public Dataset<Row> getBatchData(
      FeatureView featureView, Date startTime, Date endTime, Map<String, String> readOptions,
      Integer trainingDataVersion
  ) throws FeatureStoreException, IOException {
    return getBatchQuery(featureView, startTime, endTime, false, trainingDataVersion, Query.class)
        .read(false, readOptions);
  }

  @Override
  public Query getBatchQuery(FeatureView featureView, Date startTime, Date endTime, Boolean withLabels,
                             Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return getBatchQuery(featureView, startTime, endTime, false, trainingDataVersion, Query.class);
  }

  @Override
  public FeatureView getOrCreateFeatureView(FeatureStore featureStore, String name, Integer version,  Query query,
                                            String description, List<String> labels)
      throws FeatureStoreException, IOException {
    FeatureView featureView;
    try {
      featureView = get(featureStore, name, version, FeatureView.class);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270181")) {
        featureView = new FeatureView.FeatureViewBuilder(featureStore)
            .name(name)
            .version(version)
            .query(query)
            .description(description)
            .labels(labels)
            .build();
      } else {
        throw e;
      }
    }
    return featureView;
  }
}
