package com.logicalclocks.hsfs.engine;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.TrainingDatasetBundle;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.metadata.FeatureViewApi;
import com.logicalclocks.hsfs.metadata.Statistics;
import com.logicalclocks.hsfs.metadata.TagsApi;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.logicalclocks.hsfs.TrainingDatasetType.IN_MEMORY_TRAINING_DATASET;

public class FeatureViewEngine {

  private FeatureViewApi featureViewApi = new FeatureViewApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_VIEW);
  private TrainingDatasetEngine trainingDatasetEngine = new TrainingDatasetEngine();
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureViewEngine.class);
  private StatisticsEngine statisticsEngine = new StatisticsEngine(EntityEndpointType.TRAINING_DATASET);

  public FeatureView save(FeatureView featureView) throws FeatureStoreException, IOException {
    featureView.setFeatures(makeLabelFeatures(featureView.getLabels()));
    FeatureView updatedFeatureView = featureViewApi.save(featureView);
    featureView.setVersion(updatedFeatureView.getVersion());
    featureView.setFeatures(updatedFeatureView.getFeatures());
    return featureView;
  }

  private List<TrainingDatasetFeature> makeLabelFeatures(List<String> labels) {
    if (labels == null || labels.isEmpty()) {
      return Lists.newArrayList();
    } else {
      return labels.stream().map(label -> new TrainingDatasetFeature(label.toLowerCase(), true))
          .collect(Collectors.toList());
    }
  }

  public FeatureView update(FeatureView featureView) throws FeatureStoreException,
      IOException {
    FeatureView featureViewUpdated = featureViewApi.update(featureView);
    featureView.setDescription(featureViewUpdated.getDescription());
    return featureView;
  }

  public FeatureView get(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    FeatureView featureView = featureViewApi.get(featureStore, name, version);
    featureView.setFeatureStore(featureStore);
    featureView.getFeatures().stream()
        .filter(f -> f.getFeaturegroup() != null)
        .forEach(f -> f.getFeaturegroup().setFeatureStore(featureStore));
    featureView.getQuery().getLeftFeatureGroup().setFeatureStore(featureStore);
    return featureView;
  }

  public List<FeatureView> get(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    List<FeatureView> featureViews = featureViewApi.get(featureStore, name);
    for (FeatureView fv : featureViews) {
      fv.setFeatureStore(featureStore);
      fv.getFeatures().stream()
          .filter(f -> f.getFeaturegroup() != null)
          .forEach(f -> f.getFeaturegroup().setFeatureStore(featureStore));
      fv.getQuery().getLeftFeatureGroup().setFeatureStore(featureStore);
    }
    return featureViews;
  }

  public void delete(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStore, name);
  }

  public void delete(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStore, name, version);
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
    Dataset<Row> dataset = readDataset(featureView, trainingDataset, Maps.newHashMap());
    // Build write options map
    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(userWriteOptions, trainingDataset.getDataFormat());
    Dataset<Row>[] datasets = SparkEngine.getInstance().write(trainingDataset, dataset, writeOptions,
        SaveMode.Overwrite);
    computeStatistics(featureView, trainingDataset, datasets);
  }

  public TrainingDatasetBundle getTrainingDataset(
      FeatureView featureView, TrainingDataset trainingDataset, Map<String, String> userReadOptions
  ) throws IOException, FeatureStoreException {
    setTrainSplit(trainingDataset);
    TrainingDataset trainingDatasetUpdated = null;
    if (trainingDataset.getVersion() != null) {
      try {
        trainingDatasetUpdated = getTrainingDataMetadata(featureView, trainingDataset.getVersion());
      } catch (Exception e) {
        if (!e.getMessage().matches(".*\"errorCode\":270012.*")) {
          throw e;
        }
      }
    }
    if (trainingDatasetUpdated == null) {
      trainingDatasetUpdated = createTrainingDataMetadata(featureView, trainingDataset);
    }
    if (!IN_MEMORY_TRAINING_DATASET.equals(trainingDatasetUpdated.getTrainingDatasetType())) {
      try {
        List<Split> splits = trainingDatasetUpdated.getSplits();
        if (splits != null && !splits.isEmpty()) {
          Map<String, Dataset<Row>> datasets = Maps.newHashMap();
          for (Split split : splits) {
            datasets.put(split.getName(),
                trainingDatasetEngine.read(trainingDatasetUpdated, split.getName(), userReadOptions));
          }
          return new TrainingDatasetBundle(trainingDatasetUpdated.getVersion(),
              datasets, trainingDatasetUpdated.getTrainSplit());
        } else {
          return new TrainingDatasetBundle(trainingDatasetUpdated.getVersion(),
              trainingDatasetEngine.read(trainingDatasetUpdated, "", userReadOptions)
          );
        }
      } catch (InvalidInputException e) {
        throw new IllegalStateException(
            "Failed to read datasets. Check if path exists or recreate a training dataset."
        );
      }
    } else {
      Dataset<Row> dataset = readDataset(featureView, trainingDatasetUpdated, userReadOptions);
      TrainingDatasetBundle trainingDatasetBundle;
      if (trainingDatasetUpdated.getSplits() != null && !trainingDatasetUpdated.getSplits().isEmpty()) {
        Dataset<Row>[] datasets = SparkEngine.getInstance().splitDataset(trainingDatasetUpdated, dataset);
        trainingDatasetBundle = new TrainingDatasetBundle(trainingDatasetUpdated.getVersion(),
            convertSplitDatasetsToMap(trainingDatasetUpdated.getSplits(), datasets),
            trainingDatasetUpdated.getTrainSplit());
        computeStatistics(featureView, trainingDatasetUpdated, datasets);
      } else {
        trainingDatasetBundle = new TrainingDatasetBundle(trainingDatasetUpdated.getVersion(), dataset);
        computeStatistics(featureView, trainingDatasetUpdated, new Dataset[] {dataset});
      }
      return trainingDatasetBundle;
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
    return featureViewApi.createTrainingData(
        featureView.getName(), featureView.getVersion(), trainingDataset);
  }

  private TrainingDataset getTrainingDataMetadata(
      FeatureView featureView, Integer trainingDatasetVersion) throws IOException, FeatureStoreException {
    return featureViewApi.getTrainingData(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion(), trainingDatasetVersion);
  }

  public Statistics computeStatistics(FeatureView featureView, TrainingDataset trainingDataset,
      Dataset<Row>[] datasets)
      throws FeatureStoreException, IOException {
    if (trainingDataset.getStatisticsConfig().getEnabled()) {
      if (trainingDataset.getSplits() != null && !trainingDataset.getSplits().isEmpty()) {
        return statisticsEngine.registerSplitStatistics(
            featureView, trainingDataset, convertSplitDatasetsToMap(trainingDataset.getSplits(), datasets));
      } else {
        return statisticsEngine.computeStatistics(featureView, trainingDataset, datasets[0]);
      }
    }
    return null;
  }

  private Map<String, Dataset<Row>> convertSplitDatasetsToMap(List<Split> splits, Dataset<Row>[] datasets) {
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
    Query query = getBatchQuery(featureView, trainingDataset.getEventStartTime(), trainingDataset.getEventEndTime());
    return (Dataset<Row>) query.read(false, userReadOptions);
  }

  public void deleteTrainingData(FeatureView featureView, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    featureViewApi.deleteTrainingData(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion(), trainingDataVersion);
  }

  public void deleteTrainingData(FeatureView featureView) throws FeatureStoreException, IOException {
    featureViewApi.deleteTrainingData(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion());
  }

  public void deleteTrainingDatasetOnly(FeatureView featureView, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    featureViewApi.deleteTrainingDatasetOnly(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion(), trainingDataVersion);
  }

  public void deleteTrainingDatasetOnly(FeatureView featureView) throws FeatureStoreException, IOException {
    featureViewApi.deleteTrainingDatasetOnly(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion());
  }

  public String getBatchQueryString(FeatureView featureView, Date startTime, Date endTime)
      throws FeatureStoreException, IOException {
    Query query = getBatchQuery(featureView, startTime, endTime);
    return query.sql();
  }

  public Query getBatchQuery(FeatureView featureView, Date startTime, Date endTime)
      throws FeatureStoreException, IOException {
    Query query = featureViewApi.getBatchQuery(
        featureView.getFeatureStore(),
        featureView.getName(),
        featureView.getVersion(),
        startTime == null ? null : startTime.getTime(),
        endTime == null ? null : endTime.getTime()
    );
    query.getLeftFeatureGroup().setFeatureStore(featureView.getQuery().getLeftFeatureGroup().getFeatureStore());
    return query;
  }

  public Dataset<Row> getBatchData(
      FeatureView featureView, Date startTime, Date endTime, Map<String, String> readOptions
  ) throws FeatureStoreException, IOException {
    return (Dataset<Row>) getBatchQuery(featureView, startTime, endTime).read(false, readOptions);
  }

  public void addTag(FeatureView featureView, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureView, name, value);
  }

  public void addTag(FeatureView featureView, String name, Object value, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureView, trainingDataVersion, name, value);
  }

  public void deleteTag(FeatureView featureView, String name)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureView, name);
  }

  public void deleteTag(FeatureView featureView, String name, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureView, trainingDataVersion, name);
  }

  public Object getTag(FeatureView featureView, String name)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, name);
  }

  public Object getTag(FeatureView featureView, String name, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, trainingDataVersion, name);
  }

  public Map<String, Object> getTags(FeatureView featureView)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView);
  }

  public Map<String, Object> getTags(FeatureView featureView, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, trainingDataVersion);
  }
}
