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
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDataset;
import lombok.NonNull;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class TagsApi {

  public static final String ENTITY_ROOT_PATH = "{/entityType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  public static final String TAGS_PATH = ENTITY_ID_PATH + "/tags{/name}{?value}";

  private static final Logger LOGGER = LoggerFactory.getLogger(TagsApi.class);

  private EntityEndpointType entityType;

  public TagsApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  private void add(Integer projectId, Integer featurestoreId, Integer entityId, String name, String value)
      throws FeatureStoreException, IOException {

    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TAGS_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .set("name", name);

    if (value != null) {
      uriTemplate.set("value", value);
    }

    LOGGER.info("Sending metadata request: " + uriTemplate.expand());
    HttpPut putRequest = new HttpPut(uriTemplate.expand());
    hopsworksClient.handleRequest(putRequest);
  }

  public void add(FeatureGroupBase featureGroupBase, String name, String value)
      throws FeatureStoreException, IOException {
    add(featureGroupBase.getFeatureStore().getProjectId(), featureGroupBase.getFeatureStore().getId(),
        featureGroupBase.getId(), name, value);
  }

  public void add(TrainingDataset trainingDataset, String name, String value)
      throws FeatureStoreException, IOException {
    add(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
        trainingDataset.getId(), name, value);
  }

  private Map<String, String> get(Integer projectId, Integer featurestoreId, Integer entityId, String name)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TAGS_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId);

    if (!Strings.isNullOrEmpty(name)) {
      uriTemplate.set("name", name);
    }

    String uri = uriTemplate.expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    Tags tags = hopsworksClient.handleRequest(getRequest, Tags.class);

    return tags.getItems().stream()
        .collect(Collectors.toMap(Tags::getName, Tags::getValue));
  }

  public Map<String, String> get(FeatureGroupBase featureGroupBase, String name)
      throws FeatureStoreException, IOException {
    return get(featureGroupBase.getFeatureStore().getProjectId(), featureGroupBase.getFeatureStore().getId(),
        featureGroupBase.getId(), name);
  }

  public Map<String, String> get(TrainingDataset trainingDataset, String name)
      throws FeatureStoreException, IOException {
    return get(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
        trainingDataset.getId(), name);
  }

  private void deleteTag(Integer projectId, Integer featurestoreId, Integer entityId, String name)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TAGS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .set("name", name)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpDelete httpDelete = new HttpDelete(uri);
    hopsworksClient.handleRequest(httpDelete);
  }

  public void deleteTag(FeatureGroupBase featureGroup, String name) throws FeatureStoreException, IOException {
    deleteTag(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
        featureGroup.getId(), name);
  }

  public void deleteTag(TrainingDataset trainingDataset, String name) throws FeatureStoreException, IOException {
    deleteTag(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
        trainingDataset.getId(), name);
  }
}
