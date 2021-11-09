/*
 * Copyright (c) 2021. Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import com.logicalclocks.hsfs.StreamFeatureGroup;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;

public class StreamFeatureGroupApi {

  public static final String FEATURE_GROUP_ROOT_PATH = "/featuregroups";
  public static final String FEATURE_GROUP_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgName}{?version}";
  public static final String FEATURE_GROUP_ID_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgId}{?updateStatsConfig,"
            + "updateMetadata,validationType}";
  public static final String FEATURE_GROUP_COMMIT_PATH = FEATURE_GROUP_ID_PATH
            + "/commits{?filter_by,sort_by,offset,limit}";
  public static final String FEATURE_GROUP_CLEAR_PATH = FEATURE_GROUP_ID_PATH + "/clear";
  public static final String FEATURE_GROUP_DELTASTREAMER_PATH = FEATURE_GROUP_ID_PATH + "/deltastreamer";

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamFeatureGroup.class);

  // TODO (davit): duplicated code with FeatureGroupApi
  public StreamFeatureGroup save(StreamFeatureGroup featureGroup) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String featureGroupJson = hopsworksClient.getObjectMapper().writeValueAsString(featureGroup);

    return saveInternal(featureGroup, new StringEntity(featureGroupJson), StreamFeatureGroup.class);
  }

  // TODO (davit): duplicated code with FeatureGroupApi
  private <T> T saveInternal(FeatureGroupBase featureGroupBase,
                             StringEntity entity, Class<T> fgType) throws FeatureStoreException, IOException {
    String pathTemplate = PROJECT_PATH
            + FeatureStoreApi.FEATURE_STORE_PATH
            + FEATURE_GROUP_ROOT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
            .set("projectId", featureGroupBase.getFeatureStore().getProjectId())
            .set("fsId", featureGroupBase.getFeatureStore().getId())
            .expand();

    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(entity);

    LOGGER.info("Sending metadata request: " + uri);

    return HopsworksClient.getInstance().handleRequest(postRequest, fgType);
  }

  public void deltaStreamerJob(StreamFeatureGroup streamFeatureGroup, Map<String, String> writeOptions)
            throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
                + FeatureStoreApi.FEATURE_STORE_PATH
                + FEATURE_GROUP_DELTASTREAMER_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
                .set("projectId", streamFeatureGroup.getFeatureStore().getProjectId())
                .set("fsId", streamFeatureGroup.getFeatureStore().getId())
                .set("fgId", streamFeatureGroup.getId());

    String uri = uriTemplate.expand();

    List<Option> options = new ArrayList<>();
    for (String key: writeOptions.keySet()) {
      Option option = new Option();
      option.setName(key);
      option.setValue(writeOptions.get(key));
      options.add(option);
    }
    HsfsUtilJobConf hsfsUtilJobConf = new HsfsUtilJobConf();
    hsfsUtilJobConf.setWriteOptions(options);

    String hsfsUtilJobConftJson = hopsworksClient.getObjectMapper().writeValueAsString(hsfsUtilJobConf);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(hsfsUtilJobConftJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(hsfsUtilJobConftJson);

    LOGGER.info("Sending metadata request: " + uri);
    hopsworksClient.handleRequest(postRequest);
  }
}
