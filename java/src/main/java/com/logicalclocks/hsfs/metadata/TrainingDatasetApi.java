/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDataset;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class TrainingDatasetApi {

  private static final String TRAINING_DATASETS_PATH = "/trainingdatasets";
  private static final String TRAINING_DATASET_PATH = TRAINING_DATASETS_PATH + "{/tdName}{?version}";
  public static final String TRAINING_DATASET_ID_PATH = TRAINING_DATASETS_PATH + "{/tdId}";
  public static final String TRAINING_DATASET_TAGS_PATH = TRAINING_DATASET_ID_PATH + "/tags{/name}{?value}";

  private static final Logger LOGGER = LoggerFactory.getLogger(TrainingDatasetApi.class);

  public TrainingDataset get(FeatureStore featureStore, String tdName, Integer tdVersion)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASET_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("tdName", tdName)
        .set("version", tdVersion)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    TrainingDataset[] trainingDatasets = hopsworksClient.handleRequest(new HttpGet(uri), TrainingDataset[].class);

    // There can be only one single training dataset with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    TrainingDataset resultTd = trainingDatasets[0];
    resultTd.setFeatureStore(featureStore);
    return resultTd;
  }

  public TrainingDataset createTrainingDataset(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASETS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDataset.getFeatureStore().getProjectId())
        .set("fsId", trainingDataset.getFeatureStore().getId())
        .expand();

    String trainingDatasetJson = hopsworksClient.getObjectMapper().writeValueAsString(trainingDataset);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(trainingDatasetJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(trainingDatasetJson);
    return hopsworksClient.handleRequest(postRequest, TrainingDataset.class);
  }


  public void addTag(TrainingDataset trainingDataset, String name, String value)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
      + FeatureStoreApi.FEATURE_STORE_PATH
      + TRAINING_DATASET_TAGS_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
      .set("projectId", trainingDataset.getFeatureStore().getProjectId())
      .set("fsId", trainingDataset.getFeatureStore().getId())
      .set("fgId", trainingDataset.getId())
      .set("name", name);

    if (value != null) {
      uriTemplate.set("value", value);
    }

    LOGGER.info("Sending metadata request: " + uriTemplate.expand());
    HttpPut putRequest = new HttpPut(uriTemplate.expand());
    hopsworksClient.handleRequest(putRequest);
  }

  public String getTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
      + FeatureStoreApi.FEATURE_STORE_PATH
      + TRAINING_DATASET_TAGS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
      .set("projectId", trainingDataset.getFeatureStore().getProjectId())
      .set("fsId", trainingDataset.getFeatureStore().getId())
      .set("fgId", trainingDataset.getId())
      .set("name", name)
      .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    Tags tags = hopsworksClient.handleRequest(getRequest, Tags.class);

    return tags.getItems().get(0).getValue();
  }

  public Map<String, String> getTags(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
      + FeatureStoreApi.FEATURE_STORE_PATH
      + TRAINING_DATASET_TAGS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
      .set("projectId", featureGroup.getFeatureStore().getProjectId())
      .set("fsId", featureGroup.getFeatureStore().getId())
      .set("fgId", featureGroup.getId())
      .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    Tags tags = hopsworksClient.handleRequest(getRequest, Tags.class);

    return tags.getItems().stream()
      .collect(Collectors.toMap(Tags::getName, Tags::getValue));
  }

  public void deleteTag(FeatureGroup featureGroup, String name) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
      + FeatureStoreApi.FEATURE_STORE_PATH
      + TRAINING_DATASET_TAGS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
      .set("projectId", featureGroup.getFeatureStore().getProjectId())
      .set("fsId", featureGroup.getFeatureStore().getId())
      .set("fgId", featureGroup.getId())
      .set("name", name)
      .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpDelete httpDelete = new HttpDelete(uri);
    hopsworksClient.handleRequest(httpDelete);
  }
}
