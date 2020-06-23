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
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.StorageConnectorType;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class StorageConnectorApi {

  private static final String CONNECTOR_PATH = "/storageconnectors";
  private static final String CONNECTOR_TYPE_PATH = CONNECTOR_PATH + "{/connType}";
  private static final String ONLINE_CONNECTOR_PATH = CONNECTOR_PATH + "/onlinefeaturestore";

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageConnectorApi.class);

  public StorageConnector getByNameAndType(FeatureStore featureStore, String name, StorageConnectorType type)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + CONNECTOR_TYPE_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("connType", type)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    StorageConnector[] storageConnectors = hopsworksClient.handleRequest(new HttpGet(uri), StorageConnector[].class);

    return Arrays.stream(storageConnectors).filter(s -> s.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new FeatureStoreException("Could not find storage connector " + name + " with type " + type));
  }

  public StorageConnector getOnlineStorageConnector(FeatureStore featureStore)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + ONLINE_CONNECTOR_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), StorageConnector.class);
  }

}
