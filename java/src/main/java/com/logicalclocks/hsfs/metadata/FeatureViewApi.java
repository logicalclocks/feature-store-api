package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.constructor.ServingPreparedStatement;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FeatureViewApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureViewApi.class);

  private static final String FEATURE_VIEWS_ROOT_PATH = HopsworksClient.PROJECT_PATH
      + FeatureStoreApi.FEATURE_STORE_PATH + "/featureview";
  private static final String FEATURE_VIEWS_PATH = FEATURE_VIEWS_ROOT_PATH + "{/fvName}";
  private static final String FEATURE_VIEW_PATH = FEATURE_VIEWS_PATH + "/version{/fvVersion}";
  private static final String FEATURE_VIEW_QUERY_PATH = FEATURE_VIEWS_PATH + "/version{/fvVersion}/query";
  private static final String FEATURE_VIEW_BATCH_QUERY_PATH = FEATURE_VIEWS_PATH + "/version{/fvVersion}/query/batch";
  private static final String ALL_TRAINING_DATA_PATH = FEATURE_VIEW_PATH + "/trainingdatasets";
  private static final String TRAINING_DATA_PATH = ALL_TRAINING_DATA_PATH + "/version{/tdVersion}";
  private static final String ALL_TRAINING_DATASET_PATH = FEATURE_VIEW_PATH + "/trainingdatasets/data";
  private static final String TRAINING_DATASET_PATH = ALL_TRAINING_DATA_PATH + "/version{/tdVersion}/data";
  private static final String TRANSFORMATION_PATH = FEATURE_VIEW_PATH + "/transformation";
  private static final String PREPARED_STATEMENT_PATH = FEATURE_VIEW_PATH + "/preparedstatement{?batch}";

  public FeatureView save(FeatureView featureView) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(FEATURE_VIEWS_ROOT_PATH)
        .set("projectId", featureView.getFeatureStore().getProjectId())
        .set("fsId", featureView.getFeatureStore().getId())
        .expand();

    String featureViewJson = hopsworksClient.getObjectMapper().writeValueAsString(featureView);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(featureViewJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(featureViewJson);
    return hopsworksClient.handleRequest(postRequest, FeatureView.class);
  }

  public FeatureView get(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    String uri = UriTemplate.fromTemplate(FEATURE_VIEW_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .expand();
    Map<String, Object> params = Maps.newHashMap();
    params.put("expand", Lists.newArrayList("query", "features"));
    uri = addQueryParam(uri, params);
    HttpGet request = new HttpGet(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    return hopsworksClient.handleRequest(request, FeatureView.class);
  }

  public List<FeatureView> get(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    String uri = UriTemplate.fromTemplate(FEATURE_VIEWS_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", name)
        .expand();
    Map<String, Object> params = Maps.newHashMap();
    params.put("expand", Lists.newArrayList("query", "features"));
    uri = addQueryParam(uri, params);
    HttpGet request = new HttpGet(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    return Arrays.stream(hopsworksClient.handleRequest(request, FeatureView[].class)).collect(Collectors.toList());
  }

  private String addQueryParam(String baseUrl, Map<String, Object> params) {
    String url = baseUrl + "?";
    List<String> paramUrl = params.entrySet().stream().flatMap(entry -> {
      if (entry.getValue() instanceof String) {
        return Stream.of(entry.getKey() + "=" + entry.getValue());
      } else if (entry.getValue() instanceof List) {
        return ((List<String>) entry.getValue()).stream()
            .map(v -> entry.getKey() + "=" + v);
      } else {
        return Stream.empty();
      }
    }).collect(Collectors.toList());
    return url + Joiner.on("&").join(paramUrl);
  }

  public FeatureView update(FeatureView featureView) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(FEATURE_VIEW_PATH)
        .set("projectId", featureView.getFeatureStore().getProjectId())
        .set("fsId", featureView.getFeatureStore().getId())
        .set("fvName", featureView.getName())
        .set("fvVersion", featureView.getVersion())
        .expand();

    HttpPut request = new HttpPut(uri);
    String featureViewJson = hopsworksClient.getObjectMapper().writeValueAsString(featureView);
    request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    request.setEntity(new StringEntity(featureViewJson));
    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(request, FeatureView.class);
  }

  public void delete(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(FEATURE_VIEW_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    hopsworksClient.handleRequest(request);
  }

  public void delete(FeatureStore featureStore, String name) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(FEATURE_VIEWS_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", name)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    hopsworksClient.handleRequest(request);
  }

  public List<TransformationFunctionAttached> getTransformationFunctions(FeatureView featureView)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(TRANSFORMATION_PATH)
        .set("projectId", featureView.getFeatureStore().getProjectId())
        .set("fsId", featureView.getFeatureStore().getId())
        .set("fvName", featureView.getName())
        .set("fvVersion", featureView.getVersion())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    TransformationFunctionAttached transformationFunctionAttached =
        hopsworksClient.handleRequest(new HttpGet(uri), TransformationFunctionAttached.class);
    return transformationFunctionAttached.getItems();
  }

  public List<ServingPreparedStatement> getServingPreparedStatement(FeatureView featureView, boolean batch)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(PREPARED_STATEMENT_PATH)
        .set("projectId", featureView.getFeatureStore().getProjectId())
        .set("fsId", featureView.getFeatureStore().getId())
        .set("fvName", featureView.getName())
        .set("fvVersion", featureView.getVersion())
        .set("batch", batch)
        .expand();
    LOGGER.info("Sending metadata request: " + uri);
    ServingPreparedStatement servingPreparedStatement = hopsworksClient.handleRequest(new HttpGet(uri),
        ServingPreparedStatement.class);
    return servingPreparedStatement.getItems();
  }

  public TrainingDataset createTrainingData(String featureViewName, Integer featureViewVersion,
      TrainingDataset trainingData) throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(ALL_TRAINING_DATA_PATH)
        .set("projectId", trainingData.getFeatureStore().getProjectId())
        .set("fsId", trainingData.getFeatureStore().getId())
        .set("fvName", featureViewName)
        .set("fvVersion", featureViewVersion)
        .expand();

    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String trainingDataJson = hopsworksClient.getObjectMapper().writeValueAsString(trainingData);
    HttpPost request = new HttpPost(uri);
    request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    request.setEntity(new StringEntity(trainingDataJson));

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(request, TrainingDataset.class);
  }

  public TrainingDataset getTrainingData(FeatureStore featureStore, String featureViewName, Integer featureViewVersion,
      Integer trainingDataVersion) throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(TRAINING_DATA_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", featureViewName)
        .set("fvVersion", featureViewVersion)
        .set("tdVersion", trainingDataVersion)
        .expand();

    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    HttpGet request = new HttpGet(uri);
    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(request, TrainingDataset.class);
  }

  public void deleteTrainingData(FeatureStore featureStore, String featureViewName,
      Integer featureViewVersion, Integer trainingDataVersion) throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(TRAINING_DATA_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", featureViewName)
        .set("fvVersion", featureViewVersion)
        .set("tdVersion", trainingDataVersion)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    hopsworksClient.handleRequest(request);
  }

  public void deleteTrainingData(FeatureStore featureStore, String name, Integer version)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(ALL_TRAINING_DATA_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    hopsworksClient.handleRequest(request);
  }

  public void deleteTrainingDatasetOnly(FeatureStore featureStore, String name,
      Integer version, Integer trainingDataVersion) throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(TRAINING_DATASET_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .set("tdVersion", trainingDataVersion)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    hopsworksClient.handleRequest(request);
  }

  public void deleteTrainingDatasetOnly(FeatureStore featureStore, String name, Integer version)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(ALL_TRAINING_DATASET_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    hopsworksClient.handleRequest(request);
  }

  public Query getBatchQuery(FeatureStore featureStore, String name, Integer version,
      Long startTime, Long endTime) throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(FEATURE_VIEW_BATCH_QUERY_PATH)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .set("startTime", startTime)
        .set("endTime", endTime)
        .expand();

    HttpGet request = new HttpGet(uri);
    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    return hopsworksClient.handleRequest(request, Query.class);
  }

}
