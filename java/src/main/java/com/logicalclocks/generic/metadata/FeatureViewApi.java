/*
 *  Copyright (c) 2022. Hopsworks AB
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

package com.logicalclocks.generic.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.logicalclocks.generic.constructor.QueryBase;
import com.logicalclocks.generic.constructor.ServingPreparedStatement;
import com.logicalclocks.generic.FeatureStoreBase;
import com.logicalclocks.generic.FeatureStoreException;
import com.logicalclocks.generic.FeatureViewBase;
import com.logicalclocks.generic.TrainingDatasetBase;
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
  private static final String FEATURE_VIEW_BATCH_QUERY_PATH = FEATURE_VIEWS_PATH + "/version{/fvVersion}/query/batch"
      + "{?with_label,start_time,end_time,td_version}";
  private static final String ALL_TRAINING_DATA_PATH = FEATURE_VIEW_PATH + "/trainingdatasets";
  private static final String TRAINING_DATA_PATH = ALL_TRAINING_DATA_PATH + "/version{/tdVersion}";
  private static final String ALL_TRAINING_DATASET_PATH = FEATURE_VIEW_PATH + "/trainingdatasets/data";
  private static final String TRAINING_DATASET_PATH = ALL_TRAINING_DATA_PATH + "/version{/tdVersion}/data";
  private static final String TRANSFORMATION_PATH = FEATURE_VIEW_PATH + "/transformation";
  private static final String PREPARED_STATEMENT_PATH = FEATURE_VIEW_PATH + "/preparedstatement{?batch}";

  public FeatureViewBase save(FeatureViewBase featureViewBase) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(FEATURE_VIEWS_ROOT_PATH)
        .set("projectId", featureViewBase.getFeatureStoreBase().getProjectId())
        .set("fsId", featureViewBase.getFeatureStoreBase().getId())
        .expand();

    String featureViewJson = hopsworksClient.getObjectMapper().writeValueAsString(featureViewBase);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(featureViewJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(featureViewJson);
    return hopsworksClient.handleRequest(postRequest, FeatureViewBase.class);
  }

  public FeatureViewBase get(FeatureStoreBase featureStoreBase, String name, Integer version)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(FEATURE_VIEW_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .expand();
    Map<String, Object> params = Maps.newHashMap();
    params.put("expand", Lists.newArrayList("query", "features"));
    uri = addQueryParam(uri, params);
    HttpGet request = new HttpGet(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    try {
      return hopsworksClient.handleRequest(request, FeatureViewBase.class);
    } catch (IOException e) {
      if (e.getMessage().contains("\"errorCode\":270009")) {
        throw new FeatureStoreException(
            "Cannot get back the feature view because the query defined is no longer valid."
            + " Some feature groups used in the query may have been deleted."
            + " You can clean up this feature view on the UI or `FeatureView.clean`."
        );
      } else {
        throw e;
      }
    }
  }

  public List<FeatureViewBase> get(FeatureStoreBase featureStoreBase, String name) throws FeatureStoreException,
      IOException {
    String uri = UriTemplate.fromTemplate(FEATURE_VIEWS_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", name)
        .expand();
    Map<String, Object> params = Maps.newHashMap();
    params.put("expand", Lists.newArrayList("query", "features"));
    uri = addQueryParam(uri, params);
    HttpGet request = new HttpGet(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    try {
      return Arrays.stream(hopsworksClient.handleRequest(request, FeatureViewBase[].class))
          .collect(Collectors.toList());
    } catch (IOException e) {
      if (e.getMessage().contains("\"errorCode\":270009")) {
        throw new FeatureStoreException(
            "Cannot get back the feature view because the query defined is no longer valid."
                + " Some feature groups used in the query may have been deleted."
                + " You can clean up this feature view on the UI or `FeatureView.clean`."
        );
      } else {
        throw e;
      }
    }
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

  public FeatureViewBase update(FeatureViewBase featureViewBase) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(FEATURE_VIEW_PATH)
        .set("projectId", featureViewBase.getFeatureStoreBase().getProjectId())
        .set("fsId", featureViewBase.getFeatureStoreBase().getId())
        .set("fvName", featureViewBase.getName())
        .set("fvVersion", featureViewBase.getVersion())
        .expand();

    HttpPut request = new HttpPut(uri);
    String featureViewJson = hopsworksClient.getObjectMapper().writeValueAsString(featureViewBase);
    request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    request.setEntity(new StringEntity(featureViewJson));
    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(request, FeatureViewBase.class);
  }

  public void delete(FeatureStoreBase featureStoreBase, String name, Integer version) throws FeatureStoreException,
      IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(FEATURE_VIEW_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    hopsworksClient.handleRequest(request);
  }

  public void delete(FeatureStoreBase featureStoreBase, String name) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(FEATURE_VIEWS_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", name)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    hopsworksClient.handleRequest(request);
  }

  public List<TransformationFunctionAttached> getTransformationFunctions(FeatureViewBase featureViewBase)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(TRANSFORMATION_PATH)
        .set("projectId", featureViewBase.getFeatureStoreBase().getProjectId())
        .set("fsId", featureViewBase.getFeatureStoreBase().getId())
        .set("fvName", featureViewBase.getName())
        .set("fvVersion", featureViewBase.getVersion())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    TransformationFunctionAttached transformationFunctionAttached =
        hopsworksClient.handleRequest(new HttpGet(uri), TransformationFunctionAttached.class);
    return transformationFunctionAttached.getItems();
  }

  public List<ServingPreparedStatement> getServingPreparedStatement(FeatureViewBase featureViewBase, boolean batch)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(PREPARED_STATEMENT_PATH)
        .set("projectId", featureViewBase.getFeatureStoreBase().getProjectId())
        .set("fsId", featureViewBase.getFeatureStoreBase().getId())
        .set("fvName", featureViewBase.getName())
        .set("fvVersion", featureViewBase.getVersion())
        .set("batch", batch)
        .expand();
    LOGGER.info("Sending metadata request: " + uri);
    ServingPreparedStatement servingPreparedStatement = hopsworksClient.handleRequest(new HttpGet(uri),
        ServingPreparedStatement.class);
    return servingPreparedStatement.getItems();
  }

  public TrainingDatasetBase createTrainingData(String featureViewName, Integer featureViewVersion,
                                                TrainingDatasetBase trainingData)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(ALL_TRAINING_DATA_PATH)
        .set("projectId", trainingData.getFeatureStoreBase().getProjectId())
        .set("fsId", trainingData.getFeatureStoreBase().getId())
        .set("fvName", featureViewName)
        .set("fvVersion", featureViewVersion)
        .expand();

    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String trainingDataJson = hopsworksClient.getObjectMapper().writeValueAsString(trainingData);
    HttpPost request = new HttpPost(uri);
    request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    request.setEntity(new StringEntity(trainingDataJson));

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(request, TrainingDatasetBase.class);
  }

  public TrainingDatasetBase getTrainingData(FeatureStoreBase featureStoreBase, String featureViewName,
                                             Integer featureViewVersion, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(TRAINING_DATA_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", featureViewName)
        .set("fvVersion", featureViewVersion)
        .set("tdVersion", trainingDataVersion)
        .expand();

    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    HttpGet request = new HttpGet(uri);
    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(request, TrainingDatasetBase.class);
  }

  public void deleteTrainingData(FeatureStoreBase featureStoreBase, String featureViewName,
                                 Integer featureViewVersion, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(TRAINING_DATA_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", featureViewName)
        .set("fvVersion", featureViewVersion)
        .set("tdVersion", trainingDataVersion)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    hopsworksClient.handleRequest(request);
  }

  public void deleteTrainingData(FeatureStoreBase featureStoreBase, String name, Integer version)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(ALL_TRAINING_DATA_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    hopsworksClient.handleRequest(request);
  }

  public void deleteTrainingDatasetOnly(FeatureStoreBase featureStoreBase, String name,
                                        Integer version, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(TRAINING_DATASET_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .set("tdVersion", trainingDataVersion)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    hopsworksClient.handleRequest(request);
  }

  public void deleteTrainingDatasetOnly(FeatureStoreBase featureStoreBase, String name, Integer version)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(ALL_TRAINING_DATASET_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .expand();

    HttpDelete request = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    hopsworksClient.handleRequest(request);
  }

  public QueryBase getBatchQuery(FeatureStoreBase featureStoreBase, String name, Integer version,
                                 Long startTime, Long endTime, Boolean withLabels, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    String uri = UriTemplate.fromTemplate(FEATURE_VIEW_BATCH_QUERY_PATH)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fvName", name)
        .set("fvVersion", version)
        .set("start_time", startTime)
        .set("end_time", endTime)
        .set("with_label", withLabels)
        .set("td_version", trainingDataVersion)
        .expand();

    HttpGet request = new HttpGet(uri);
    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    return hopsworksClient.handleRequest(request, QueryBase.class);
  }

}
