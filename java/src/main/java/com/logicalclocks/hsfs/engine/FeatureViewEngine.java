package com.logicalclocks.hsfs.engine;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.TrainingDatasetBundle;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.constructor.Join;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.metadata.FeatureViewApi;
import com.logicalclocks.hsfs.metadata.Statistics;
import com.logicalclocks.hsfs.metadata.TagsApi;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
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
  static String AMBIGUOUS_LABEL_ERROR = "Provided label '%s' is ambiguous and exists in more than one feature "
      + "groups. You can provide the label with the prefix you specify in the join.";
  static String LABEL_NOT_EXIST_ERROR = "Provided label '%s' do not exist in any of the feature groups.";

  public FeatureView save(FeatureView featureView) throws FeatureStoreException, IOException {
    featureView.setFeatures(makeLabelFeatures(featureView.getQuery(), featureView.getLabels()));
    FeatureView updatedFeatureView = featureViewApi.save(featureView);
    featureView.setVersion(updatedFeatureView.getVersion());
    featureView.setFeatures(updatedFeatureView.getFeatures());
    return featureView;
  }

  static List<TrainingDatasetFeature> makeLabelFeatures(Query query, List<String> labels) throws FeatureStoreException {
    if (labels == null || labels.isEmpty()) {
      return Lists.newArrayList();
    } else {
      // If provided label matches column with prefix, then attach label.
      // If provided label matches only one column without prefix, then attach label. (For
      // backward compatibility purpose, as of v3.0, labels are matched to columns without prefix.)
      // If provided label matches multiple columns without prefix, then raise exception because it is ambiguous.

      Map<String, String> labelWithPrefixToFeature = Maps.newHashMap();
      Map<String, FeatureGroup> labelWithPrefixToFeatureGroup = Maps.newHashMap();
      Map<String, List<FeatureGroup>> labelToFeatureGroups = Maps.newHashMap();
      for (Feature feat : query.getLeftFeatures()) {
        labelWithPrefixToFeature.put(feat.getName(), feat.getName());
        labelWithPrefixToFeatureGroup.put(feat.getName(),
            (new FeatureGroup(null, feat.getFeatureGroupId())));
      }
      for (Join join : query.getJoins()) {
        for (Feature feat : join.getQuery().getLeftFeatures()) {
          String labelWithPrefix = join.getPrefix() + feat.getName();
          labelWithPrefixToFeature.put(labelWithPrefix, feat.getName());
          labelWithPrefixToFeatureGroup.put(labelWithPrefix,
              new FeatureGroup(null, feat.getFeatureGroupId()));
          List<FeatureGroup> featureGroups = labelToFeatureGroups.getOrDefault(feat.getName(), Lists.newArrayList());
          featureGroups.add(new FeatureGroup(null, feat.getFeatureGroupId()));
          labelToFeatureGroups.put(feat.getName(), featureGroups);
        }
      }
      List<TrainingDatasetFeature> trainingDatasetFeatures = Lists.newArrayList();
      for (String label : labels) {
        if (labelWithPrefixToFeature.containsKey(label)) {
          trainingDatasetFeatures.add(new TrainingDatasetFeature(
              labelWithPrefixToFeatureGroup.get(label.toLowerCase()),
              labelWithPrefixToFeature.get(label.toLowerCase()),
              true));
        } else if (labelToFeatureGroups.containsKey(label)) {
          if (labelToFeatureGroups.get(label.toLowerCase()).size() > 1) {
            throw new FeatureStoreException(String.format(AMBIGUOUS_LABEL_ERROR, label));
          }
          trainingDatasetFeatures.add(new TrainingDatasetFeature(
              labelToFeatureGroups.get(label.toLowerCase()).get(0),
              label.toLowerCase(),
              true));
        } else {
          throw new FeatureStoreException(String.format(LABEL_NOT_EXIST_ERROR, label));
        }

      }
      return trainingDatasetFeatures;
    }
  }


  public FeatureView update(FeatureView featureView) throws FeatureStoreException,
      IOException {
    featureViewApi.update(featureView);
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
    featureView.setLabels(
        featureView.getFeatures().stream()
            .filter(TrainingDatasetFeature::getLabel)
            .map(TrainingDatasetFeature::getName)
            .collect(Collectors.toList()));
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
      fv.setLabels(
          fv.getFeatures().stream()
              .filter(TrainingDatasetFeature::getLabel)
              .map(TrainingDatasetFeature::getName)
              .collect(Collectors.toList()));
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
    // Build write options map
    Map<String, String> writeOptions =
        SparkEngine.getInstance().getWriteOptions(userWriteOptions, trainingDataset.getDataFormat());
    Query query = getBatchQuery(featureView, trainingDataset.getEventStartTime(),
        trainingDataset.getEventEndTime(), true, trainingDataset.getVersion());
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
    if (!IN_MEMORY_TRAINING_DATASET.equals(trainingDatasetUpdated.getTrainingDatasetType())) {
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
            true, trainingDataset.getVersion());
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
    return featureViewApi.createTrainingData(
        featureView.getName(), featureView.getVersion(), trainingDataset);
  }

  private void setEventTime(FeatureView featureView, TrainingDataset trainingDataset) {
    String eventTime = featureView.getQuery().getLeftFeatureGroup().getEventTime();
    if (!Strings.isNullOrEmpty(eventTime)) {
      if (trainingDataset.getSplits() != null && !trainingDataset.getSplits().isEmpty()) {
        for (Split split : trainingDataset.getSplits()) {
          if (split.getSplitType() == Split.SplitType.TIME_SERIES_SPLIT
              && split.getName().equals(Split.TRAIN)
              && split.getStartTime() == null) {
            split.setStartTime(getStartTime());
          }
          if (split.getSplitType() == Split.SplitType.TIME_SERIES_SPLIT
              && split.getName().equals(Split.TEST)
              && split.getEndTime() == null) {
            split.setEndTime(getEndTime());
          }
        }
      } else {
        if (trainingDataset.getEventStartTime() == null) {
          trainingDataset.setEventStartTime(getStartTime());
        }
        if (trainingDataset.getEventEndTime() == null) {
          trainingDataset.setEventEndTime(getEndTime());
        }
      }
    }
  }

  private Date getStartTime() {
    return new Date(1000);
  }

  private Date getEndTime() {
    return new Date();
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
    Query query = getBatchQuery(featureView, trainingDataset.getEventStartTime(), trainingDataset.getEventEndTime(),
        true, trainingDataset.getVersion());
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

  public String getBatchQueryString(FeatureView featureView, Date startTime, Date endTime, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    Query query = getBatchQuery(featureView, startTime, endTime, false, trainingDataVersion);
    return query.sql();
  }

  public Query getBatchQuery(FeatureView featureView, Date startTime, Date endTime, Boolean withLabels,
                             Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    Query query = null;
    try {
      query = featureViewApi.getBatchQuery(
          featureView.getFeatureStore(),
          featureView.getName(),
          featureView.getVersion(),
          startTime == null ? null : startTime.getTime(),
          endTime == null ? null : endTime.getTime(),
          withLabels,
          trainingDataVersion
      );
    } catch (IOException e) {
      if (e.getMessage().contains("\"errorCode\":270172")) {
        throw new FeatureStoreException(
            "Cannot generate dataset or query from the given start/end time because"
                + " event time column is not available in the left feature groups."
                + " A start/end time should not be provided as parameters."
        );
      } else {
        throw e;
      }
    }
    query.getLeftFeatureGroup().setFeatureStore(featureView.getQuery().getLeftFeatureGroup().getFeatureStore());
    return query;
  }

  public Dataset<Row> getBatchData(
      FeatureView featureView, Date startTime, Date endTime, Map<String, String> readOptions,
      Integer trainingDataVersion
  ) throws FeatureStoreException, IOException {
    return (Dataset<Row>) getBatchQuery(featureView, startTime, endTime, false, trainingDataVersion)
        .read(false, readOptions);
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

  public FeatureView getOrCreateFeatureView(FeatureStore featureStore, String name, Integer version,  Query query,
                                            String description, List<String> labels)
      throws FeatureStoreException, IOException {
    FeatureView featureView = null;
    try {
      featureView = get(featureStore, name, version);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270181")) {
        featureView = new FeatureView.FeatureViewBuilder(featureStore)
            .name(name)
            .version(version)
            .query(query)
            .description(description)
            .labels(labels)
            .build();
      }
    }
    return featureView;
  }
}
