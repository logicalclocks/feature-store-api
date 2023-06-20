/*
 * Copyright (c) 2023 Hopsworks AB
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
import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class VariablesApi {

  public static final String LOADBALANCER_EXTERNAL_DOMAIN = "loadbalancer_external_domain";

  private static final String VARIABLE_PATH = "/variables/{variableName}";
  private static final Logger LOGGER = LoggerFactory.getLogger(Variable.class);

  public Optional<Variable> get(String variableName) throws IOException, FeatureStoreException {
    String pathTemplate = HopsworksClient.API_PATH
        + VARIABLE_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("variableName", variableName);

    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    LOGGER.info("Sending metadata request: " + uriTemplate.expand());
    HttpGet getRequest = new HttpGet(uriTemplate.expand());
    try {
      return Optional.of(hopsworksClient.handleRequest(getRequest, Variable.class));
    } catch (IOException e) {
      return Optional.empty();
    }
  }
}
