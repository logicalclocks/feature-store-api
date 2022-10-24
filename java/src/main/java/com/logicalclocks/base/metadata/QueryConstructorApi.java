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
import com.logicalclocks.base.constructor.QueryBase;
import com.logicalclocks.base.FeatureStoreBase;
import com.logicalclocks.base.FeatureStoreException;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QueryConstructorApi {

  public static final String QUERY_CONSTRUCTOR_PATH = "/query";

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryConstructorApi.class);

  public <U> FsQueryBase constructQuery(FeatureStoreBase featureStoreBase, QueryBase queryBase, Class<U> fgType)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_SERVICE_PATH
        + QUERY_CONSTRUCTOR_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStoreBase.getProjectId())
        .expand();

    String queryJson = hopsworksClient.getObjectMapper().writeValueAsString(queryBase);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(queryJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info("Sending query: " + queryJson);
    FsQueryBase fsQueryBase = (FsQueryBase) hopsworksClient.handleRequest(putRequest, fgType);
    fsQueryBase.removeNewLines();
    return fsQueryBase;
  }
}
