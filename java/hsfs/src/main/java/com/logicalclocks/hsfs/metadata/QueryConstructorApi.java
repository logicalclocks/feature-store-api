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
import com.logicalclocks.hsfs.constructor.FeatureGroupAlias;
import com.logicalclocks.hsfs.constructor.FsQueryBase;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class QueryConstructorApi {

  public static final String QUERY_CONSTRUCTOR_PATH = "/query";

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryConstructorApi.class);

  public <T> FsQueryBase constructQuery(FeatureStoreBase featureStoreBase, QueryBase queryBase, Class<T> fsQueryType)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_SERVICE_PATH
        + QUERY_CONSTRUCTOR_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
    putRequest.setEntity(hopsworksClient.buildStringEntity(queryBase));

    FsQueryBase fsQueryBase = (FsQueryBase) hopsworksClient.handleRequest(putRequest, fsQueryType);
    fsQueryBase.removeNewLines();
    List<FeatureGroupAlias> onDemandFeatureGroupAliases = fsQueryBase.getOnDemandFeatureGroups();
    for (FeatureGroupAlias onDemandFeatureGroupAlias : onDemandFeatureGroupAliases) {
      FeatureGroupBase updatedFG = onDemandFeatureGroupAlias.getOnDemandFeatureGroup();
      updatedFG.setFeatureStore(featureStoreBase);
      onDemandFeatureGroupAlias.setOnDemandFeatureGroup(updatedFG);
    }
    List<FeatureGroupAlias> hudiCachedFeatureAliases = fsQueryBase.getHudiCachedFeatureGroups();
    for (FeatureGroupAlias hudiCachedFeatureAlias : hudiCachedFeatureAliases) {
      FeatureGroupBase updatedFG = hudiCachedFeatureAlias.getFeatureGroup();
      updatedFG.setFeatureStore(featureStoreBase);
      hudiCachedFeatureAlias.setFeatureGroup(updatedFG);
    }
    fsQueryBase.setOnDemandFeatureGroups(onDemandFeatureGroupAliases);
    fsQueryBase.setHudiCachedFeatureGroups(hudiCachedFeatureAliases);
    return fsQueryBase;
  }
}
