package com.logicalclocks.hsfs.engine;

import com.google.common.base.Strings;
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

import static com.logicalclocks.hsfs.TrainingDatasetType.IN_MEMORY;

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
    return labels.stream().map(label -> new TrainingDatasetFeature(label.toLowerCase(), true))
        .collect(Collectors.toList());
  }

  public FeatureView update(FeatureView featureView) throws FeatureStoreException,
      IOException {
    FeatureView featureViewUpdated = featureViewApi.update(featureView);
    featureView.setDescription(featureViewUpdated.getDescription());
    return featureView;
  }

  public FeatureView get(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    return featureViewApi.get(featureStore, name, version);
  }

  public List<FeatureView> get(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    return featureViewApi.get(featureStore, name);
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
    Dataset<Row> dataset = readDataset(featureView, trainingDataset, Maps.newHashMap());
    // Build write options map
    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(userWriteOptions, trainingDataset.getDataFormat());
    SparkEngine.getInstance().write(trainingDataset, dataset, writeOptions, SaveMode.Overwrite);
    computeStatistics(trainingDataset,
        getTrainingDataset(featureView, trainingDataset, Maps.newHashMap()).getTrainSet());
    return new TrainingDatasetBundle(trainingDataset.getVersion());
  }

  public TrainingDatasetBundle getTrainingDataset(
      FeatureView featureView, TrainingDataset trainingDataset, Map<String, String> userReadOptions
  ) throws IOException, FeatureStoreException {
    setTrainSplit(trainingDataset);
    TrainingDataset trainingDatasetUpdated = null;
    if (trainingDataset.getVersion() != null) {
      try {
        trainingDatasetUpdated = getTrainingDataMetadata(featureView, trainingDataset);
      } catch (Exception e) {
        if (!e.getMessage().matches(".*27012.*")) {
          throw e;
        }
      }
    }
    if (trainingDatasetUpdated == null) {
      trainingDatasetUpdated = createTrainingDataMetadata(featureView, trainingDataset);
    }
    if (!IN_MEMORY.equals(trainingDatasetUpdated.getTrainingDatasetType())) {
      List<Split> splits = trainingDatasetUpdated.getSplits();
      if (splits != null && !splits.isEmpty()) {
        Map<String, Dataset<Row>> datasets = Maps.newHashMap();
        for (Split split: splits) {
          datasets.put(split.getName(),
              trainingDatasetEngine.read(trainingDatasetUpdated, split.getName(), userReadOptions));
        }
        return new TrainingDatasetBundle(trainingDataset.getVersion(), datasets, trainingDataset.getTrainSplit());
      } else {
        return new TrainingDatasetBundle(trainingDataset.getVersion(),
            trainingDatasetEngine.read(trainingDatasetUpdated, "", userReadOptions)
        );
      }
    } else {
      Dataset<Row> dataset = readDataset(featureView, trainingDataset, userReadOptions);
      TrainingDatasetBundle trainingDatasetBundle;
      if (trainingDataset.getSplits() != null && !trainingDataset.getSplits().isEmpty()) {
        Dataset<Row>[] datasets = SparkEngine.getInstance().splitDataset(trainingDataset, dataset);
        trainingDatasetBundle = new TrainingDatasetBundle(trainingDataset.getVersion(),
            convertSplitDatasetsToMap(trainingDataset.getSplits(), datasets), trainingDataset.getTrainSplit());
      } else {
        trainingDatasetBundle = new TrainingDatasetBundle(trainingDataset.getVersion(), dataset);
      }
      computeStatistics(trainingDataset,
          getTrainingDataset(featureView, trainingDataset, Maps.newHashMap()).getTrainSet());
      return trainingDatasetBundle;
    }
  }

  private void setTrainSplit(TrainingDataset trainingDataset) {
    if (trainingDataset.getSplits().size() > 0 && Strings.isNullOrEmpty(trainingDataset.getTrainSplit())) {
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
      FeatureView featureView, TrainingDataset trainingDataset) throws IOException, FeatureStoreException {
    return featureViewApi.getTrainingData(featureView.getFeatureStore(), featureView.getName(),
        featureView.getVersion(), trainingDataset.getVersion());
  }

  public Statistics computeStatistics(TrainingDataset trainingDataset, Dataset<Row> dataset)
      throws FeatureStoreException,
      IOException {
    if (trainingDataset.getStatisticsConfig().getEnabled()) {
      if (trainingDataset.getSplits() != null && !trainingDataset.getSplits().isEmpty()) {
        return statisticsEngine.registerSplitStatistics(trainingDataset);
      } else {
        return statisticsEngine.computeStatistics(trainingDataset, dataset);
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

  public void recreateTrainingDataset(Integer version) {

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

  public String getBatchQueryString(FeatureView featureView) {
    return null;
  }

  public String getBatchQueryString(FeatureView featureView, String startTime, String endTime) {
    return null;
  }

  public Query getBatchQuery(FeatureView featureView, Date startTime, Date endTime)
      throws FeatureStoreException, IOException {
    return featureViewApi.getBatchQuery(
        featureView.getFeatureStore(),
        featureView.getName(),
        featureView.getVersion(),
        startTime == null ? null : startTime.getTime(),
        endTime == null ? null : endTime.getTime()
    );
  }

  public Dataset<Row> getBatchData(
      FeatureView featureView, String startTime, String endTime, Map<String, String> readOptions
  ) {
    return null;
  }

  public void addTag(FeatureView featureView, String name, Object value) {

  }

  public void addTag(FeatureView featureView, String name, Object value, Integer trainingDataVersion) {

  }

  public void deleteTag(FeatureView featureView, String name) {

  }

  public void deleteTag(FeatureView featureView, String name, Integer trainingDataVersion) {

  }

  public Object getTag(FeatureView featureView, String name) {
    return null;
  }

  public Object getTag(FeatureView featureView, String name, Integer trainingDataVersion) {
    return null;
  }

  public Map<String, Object> getTags(FeatureView featureView) {
    return null;
  }

  public Map<String, Object> getTags(FeatureView featureView, Integer trainingDataVersion) {
    return null;
  }
}
