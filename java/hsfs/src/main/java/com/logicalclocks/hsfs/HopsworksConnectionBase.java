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

package com.logicalclocks.hsfs;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.metadata.FeatureStoreApi;
import com.logicalclocks.hsfs.metadata.ProjectApi;
import com.logicalclocks.hsfs.util.Constants;

import lombok.Getter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;

import java.io.Closeable;
import java.io.IOException;

public abstract class HopsworksConnectionBase implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HopsworksConnectionBase.class);

  @Getter
  protected String host;

  @Getter
  protected int port;

  @Getter
  protected String project;

  @Getter
  protected Region region;

  @Getter
  protected SecretStore secretStore;

  @Getter
  protected boolean hostnameVerification;

  @Getter
  protected String keyStorePath;

  @Getter
  protected String trustStorePath;

  @Getter
  protected String certPath;

  @Getter
  protected String apiKeyFilePath;

  @Getter
  protected String apiKeyValue;

  protected FeatureStoreApi featureStoreApi = new FeatureStoreApi();
  protected ProjectApi projectApi = new ProjectApi();

  protected Project projectObj;

  public abstract Object getFeatureStore() throws IOException, FeatureStoreException;

  public abstract Object getFeatureStore(String name) throws IOException, FeatureStoreException;

  /**
   * Close the connection and clean up the certificates.
   */
  public void close() {
    // Close the client
  }

  public String rewriteFeatureStoreName(String name) {
    name = name.toLowerCase();
    if (name.endsWith(Constants.FEATURESTORE_SUFFIX)) {
      return name;
    } else {
      return name + Constants.FEATURESTORE_SUFFIX;
    }
  }

  public Project getProject() throws IOException, FeatureStoreException {
    LOGGER.info("Getting information for project name: " + project);
    return projectApi.get(project);
  }

  public String getProjectName(String project) {
    if (Strings.isNullOrEmpty(project)) {
      // User didn't specify a project in the connection construction. Assume they are running
      // from within Hopsworks and the project name is available a system property
      return System.getProperty(Constants.PROJECTNAME_ENV);
    }
    return project;
  }
}
