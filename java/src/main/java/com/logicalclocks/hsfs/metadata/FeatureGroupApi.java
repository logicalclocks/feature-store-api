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
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import com.logicalclocks.hsfs.OfflineFeatureGroup;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FeatureGroupApi {

  public static final String FEATURE_GROUP_ROOT_PATH = "/featuregroups";
  public static final String FEATURE_GROUP_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgName}{?version}";

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupApi.class);

  public OfflineFeatureGroup get(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fgName", fgName)
        .set("version", fgVersion)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    OfflineFeatureGroup[] offlineFeatureGroups = hopsworksClient.handleRequest(new HttpGet(uri), OfflineFeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    OfflineFeatureGroup resultFg = offlineFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }

  public OfflineFeatureGroup create(OfflineFeatureGroup offlineFeatureGroup) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_ROOT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", offlineFeatureGroup.getFeatureStore().getProjectId())
        .set("fsId", offlineFeatureGroup.getFeatureStore().getId())
        .expand();

    String featureGroupJson = hopsworksClient.getObjectMapper().writeValueAsString(offlineFeatureGroup);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(featureGroupJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(featureGroupJson);

    return hopsworksClient.handleRequest(postRequest, OfflineFeatureGroup.class);
  }
}
