/*
 *  Copyright (c) 2020-2022. Hopsworks AB
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

package com.logicalclocks.base.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.base.constructor.FsQueryBase;
import com.logicalclocks.base.constructor.ServingPreparedStatement;
import com.logicalclocks.base.FeatureStoreBase;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.TrainingDatasetBase;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TrainingDatasetApi {

  private static final String TRAINING_DATASETS_PATH = "/trainingdatasets";
  private static final String TRAINING_DATASET_PATH = TRAINING_DATASETS_PATH + "{/tdName}{?version}";
  private static final String TRAINING_QUERY_PATH = TRAINING_DATASETS_PATH + "{/tdId}/query{?withLabel}{&hiveQuery}";
  public static final String TRAINING_DATASET_ID_PATH = TRAINING_DATASETS_PATH + "{/fgId}{?updateStatsConfig,"
      + "updateMetadata}";
  private static final String PREP_STATEMENT_PATH = TRAINING_DATASETS_PATH + "{/tdId}/preparedstatements{?batch}";
  private static final String TRANSFORMATION_FUNCTION_PATH =
      TRAINING_DATASETS_PATH + "{/tdId}/transformationfunctions";

  private static final Logger LOGGER = LoggerFactory.getLogger(TrainingDatasetApi.class);

  public List<TrainingDatasetBase> get(FeatureStoreBase featureStoreBase, String tdName, Integer tdVersion)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASET_PATH;

    UriTemplate uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("tdName", tdName);

    if (tdVersion != null) {
      uri.set("version", tdVersion);
    }
    String uriString = uri.expand();

    LOGGER.info("Sending metadata request: " + uriString);
    TrainingDatasetBase[]
        trainingDatasetBases = hopsworksClient.handleRequest(new HttpGet(uriString), TrainingDatasetBase[].class);

    for (TrainingDatasetBase td : trainingDatasetBases) {
      td.setFeatureStore(featureStoreBase);
      td.getFeatures().stream()
          .filter(f -> f.getFeatureGroup() != null)
          .forEach(f -> f.getFeatureGroup().setFeatureStore(featureStoreBase));
    }
    return Arrays.asList(trainingDatasetBases);
  }

  public TrainingDatasetBase getTrainingDataset(FeatureStoreBase featureStoreBase, String tdName, Integer tdVersion)
      throws IOException, FeatureStoreException {
    // There can be only one single training dataset with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    return get(featureStoreBase, tdName, tdVersion).get(0);
  }

  public TrainingDatasetBase createTrainingDataset(TrainingDatasetBase trainingDatasetBase)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASETS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDatasetBase.getFeatureStore().getProjectId())
        .set("fsId", trainingDatasetBase.getFeatureStore().getId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setEntity(hopsworksClient.buildStringEntity(trainingDatasetBase));
    return hopsworksClient.handleRequest(postRequest, TrainingDatasetBase.class);
  }

  public FsQueryBase getQuery(TrainingDatasetBase trainingDatasetBase, boolean withLabel, boolean isHiveQuery)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_QUERY_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDatasetBase.getFeatureStore().getProjectId())
        .set("fsId", trainingDatasetBase.getFeatureStore().getId())
        .set("tdId", trainingDatasetBase.getId())
        .set("withLabel", withLabel)
        .set("hiveQuery", isHiveQuery)
        .expand();

    HttpGet getRequest = new HttpGet(uri);
    LOGGER.info("Sending metadata request: " + uri);

    return hopsworksClient.handleRequest(getRequest, FsQueryBase.class);
  }

  public List<ServingPreparedStatement> getServingPreparedStatement(TrainingDatasetBase trainingDatasetBase,
                                                                    boolean batch)
      throws FeatureStoreException, IOException {
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + PREP_STATEMENT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDatasetBase.getFeatureStore().getProjectId())
        .set("fsId", trainingDatasetBase.getFeatureStore().getId())
        .set("tdId", trainingDatasetBase.getId())
        .set("batch", batch)
        .expand();
    HttpGet getRequest = new HttpGet(uri);
    LOGGER.info("Sending metadata request: " + uri);
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    ServingPreparedStatement servingPreparedStatement = hopsworksClient.handleRequest(getRequest,
        ServingPreparedStatement.class);
    return servingPreparedStatement.getItems();
  }

  public TrainingDatasetBase updateMetadata(TrainingDatasetBase trainingDatasetBase, String queryParameter)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASET_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDatasetBase.getFeatureStore().getProjectId())
        .set("fsId", trainingDatasetBase.getFeatureStore().getId())
        .set("fgId", trainingDatasetBase.getId())
        .set(queryParameter, true)
        .expand();


    LOGGER.info("Sending metadata request: " + uri);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setEntity(hopsworksClient.buildStringEntity(trainingDatasetBase));
    return hopsworksClient.handleRequest(putRequest, TrainingDatasetBase.class);
  }

  public void delete(TrainingDatasetBase trainingDatasetBase)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASET_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDatasetBase.getFeatureStore().getProjectId())
        .set("fsId", trainingDatasetBase.getFeatureStore().getId())
        .set("fgId", trainingDatasetBase.getId())
        .expand();

    HttpDelete deleteRequest = new HttpDelete(uri);
    LOGGER.info("Sending metadata request: " + uri);

    hopsworksClient.handleRequest(deleteRequest);
  }

  public List<TransformationFunctionAttached> getTransformationFunctions(TrainingDatasetBase trainingDatasetBase)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRANSFORMATION_FUNCTION_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDatasetBase.getFeatureStore().getProjectId())
        .set("fsId", trainingDatasetBase.getFeatureStore().getId())
        .set("tdId", trainingDatasetBase.getId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    TransformationFunctionAttached transformationFunctionAttached =
        hopsworksClient.handleRequest(new HttpGet(uri), TransformationFunctionAttached.class);
    return transformationFunctionAttached.getItems();
  }
}
