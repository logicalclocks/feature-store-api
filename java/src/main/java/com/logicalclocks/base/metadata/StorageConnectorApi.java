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
import com.logicalclocks.base.FeatureStoreBase;
import com.logicalclocks.base.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StorageConnectorApi {

  private static final String CONNECTOR_PATH = "/storageconnectors";
  private static final String CONNECTOR_TYPE_PATH =
      CONNECTOR_PATH + "{/connType}{/name}{?temporaryCredentials}";
  private static final String ONLINE_CONNECTOR_PATH = CONNECTOR_PATH + "/onlinefeaturestore";

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageConnectorApi.class);

  public <U> U get(Integer featureStoreId, String name, Class<U> storageConnectorType)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + CONNECTOR_TYPE_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featureStoreId)
        .set("name", name)
        .set("temporaryCredentials", true)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), storageConnectorType);
  }

  public <U> U getByName(FeatureStoreBase featureStoreBase, String name, Class<U> storageConnectorType)
      throws IOException, FeatureStoreException {
    return get(featureStoreBase.getId(), name, storageConnectorType);
  }

  public <U> U  getOnlineStorageConnector(FeatureStoreBase featureStoreBase, Class<U> storageConnectorType)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + ONLINE_CONNECTOR_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), storageConnectorType);
  }
}
