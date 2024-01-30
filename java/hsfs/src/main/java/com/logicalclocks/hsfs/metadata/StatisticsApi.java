/*
 *  Copyright (c) 2020-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.TrainingDatasetBase;
import lombok.NonNull;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StatisticsApi {

  private static final String ENTITY_ROOT_PATH = "{/entityType}";
  private static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  private static final String STATISTICS_PATH = ENTITY_ID_PATH + "/statistics{?filter_by,fields,sort_by,offset,limit}";
  private static final String FV_STATISTICS_PATH = "/featureview{/fvName}/version{/fvVersion}"
      + "/trainingdatasets/version{/tdVersion}/statistics{?filter_by,fields,sort_by,offset,limit}";

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsApi.class);

  private EntityEndpointType entityType;

  public StatisticsApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  public Statistics post(FeatureGroupBase featureGroup, Statistics statistics)
      throws FeatureStoreException, IOException {
    return post(featureGroup.getFeatureStore().getId(),
        featureGroup.getId(), statistics);
  }

  public Statistics post(TrainingDatasetBase trainingDatasetBase, Statistics statistics)
      throws FeatureStoreException, IOException {
    return post(trainingDatasetBase.getFeatureStore().getId(),
        trainingDatasetBase.getId(), statistics);
  }

  private Statistics post(Integer featurestoreId, Integer entityId, Statistics statistics)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + STATISTICS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .expand();
    return post(uri, statistics);
  }

  public Statistics post(FeatureViewBase featureViewBase,
                         Integer trainingDataVersion, Statistics statistics)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + FV_STATISTICS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featureViewBase.getFeatureStore().getId())
        .set("fvName", featureViewBase.getName())
        .set("fvVersion", featureViewBase.getVersion())
        .set("tdVersion", trainingDataVersion)
        .expand();
    return post(uri, statistics);
  }

  private Statistics post(String uri, Statistics statistics) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    LOGGER.info("Sending metadata request: " + uri);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setEntity(hopsworksClient.buildStringEntity(statistics));
    return hopsworksClient.handleRequest(postRequest, Statistics.class);
  }

  public Statistics get(FeatureGroupBase featureGroup, String commitTime) throws FeatureStoreException, IOException {
    return get(featureGroup.getFeatureStore().getId(),
        featureGroup.getId(), commitTime);
  }

  public Statistics get(TrainingDatasetBase trainingDatasetBase, String commitTime)
      throws FeatureStoreException, IOException {
    return get(trainingDatasetBase.getFeatureStore().getId(),
        trainingDatasetBase.getId(), commitTime);
  }

  private Statistics get(Integer featurestoreId, Integer entityId, String commitTime)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + STATISTICS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .set("filter_by", "computation_time_eq:" + commitTime)
        .set("fields", "content")
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    Statistics statistics = hopsworksClient.handleRequest(getRequest, Statistics.class);

    // currently getting multiple commits at the same time is not allowed
    if (statistics.getItems().size() == 1) {
      return statistics.getItems().get(0);
    }
    return null;
  }

  public Statistics getLast(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    return getLast(featureGroup.getFeatureStore().getId(),
        featureGroup.getId());
  }

  public Statistics getLast(TrainingDatasetBase trainingDatasetBase) throws FeatureStoreException, IOException {
    return getLast(trainingDatasetBase.getFeatureStore().getId(),
        trainingDatasetBase.getId());
  }

  private Statistics getLast(Integer featurestoreId, Integer entityId)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + STATISTICS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .set("sort_by", "computation_time:desc")
        .set("offset", 0)
        .set("limit", 1)
        .set("fields", "content")
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    Statistics statistics = hopsworksClient.handleRequest(getRequest, Statistics.class);

    // currently getting multiple commits at the same time is not allowed
    if (statistics.getItems().size() == 1) {
      return statistics.getItems().get(0);
    }
    return null;
  }

}
