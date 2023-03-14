/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HopsworksConnectionBase;
import com.logicalclocks.hsfs.SecretStore;
import com.logicalclocks.hsfs.metadata.HopsworksClient;

import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import lombok.Builder;

import software.amazon.awssdk.regions.Region;

import java.io.IOException;

public class HopsworksConnection extends HopsworksConnectionBase {

  @Builder
  public HopsworksConnection(String host, int port, String project, Region region, SecretStore secretStore,
                             boolean hostnameVerification, String trustStorePath,
                             String certPath, String apiKeyFilePath, String apiKeyValue)
      throws IOException, FeatureStoreException {
    this.host = host;
    this.port = port;
    this.project = getProjectName(project);
    this.region = region;
    this.secretStore = secretStore;
    this.hostnameVerification = hostnameVerification;
    this.trustStorePath = trustStorePath;
    this.certPath = certPath;
    this.apiKeyFilePath = apiKeyFilePath;
    this.apiKeyValue = apiKeyValue;

    String frameworkTrustStorePath = null;
    String keyStorePath = null;
    String certKey = null;
    if (!System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS)) {
      SparkEngine.getInstance().validateSparkConfiguration();
      frameworkTrustStorePath = SparkEngine.getInstance().getTrustStorePath();
      keyStorePath = SparkEngine.getInstance().getKeyStorePath();
      certKey = SparkEngine.getInstance().getCertKey();
    }
    HopsworksClient.setupHopsworksClient(host, port, region, secretStore,
        hostnameVerification, trustStorePath, this.apiKeyFilePath, this.apiKeyValue,
        frameworkTrustStorePath,
        keyStorePath,
        certKey);
    this.projectObj = getProject();
    HopsworksClient.getInstance().setProject(this.projectObj);
  }

  /**
   * Retrieve the project feature store.
   *
   * @return FeatureStore
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   */
  @Override
  public FeatureStore getFeatureStore() throws IOException, FeatureStoreException {
    return getFeatureStore(rewriteFeatureStoreName(project));
  }

  /**
   * Retrieve a feature store based on name. The feature store needs to be shared with
   * the connection's project. The name is the project name of the feature store.
   *
   * @param name the name of the feature store to get the handle for
   * @return FeatureStore
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   */
  @Override
  public FeatureStore getFeatureStore(String name) throws IOException, FeatureStoreException {
    return featureStoreApi.get(projectObj.getProjectId(), rewriteFeatureStoreName(name), FeatureStore.class);
  }
}
