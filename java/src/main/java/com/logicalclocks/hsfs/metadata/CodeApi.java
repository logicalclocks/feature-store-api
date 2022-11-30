/*
 * Copyright (c) 2021 Logical Clocks AB
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
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDataset;
import lombok.NonNull;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class CodeApi {

  public static final String ENTITY_ROOT_PATH = "{/dataSetType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/dataSetId}";
  public static final String CODE_PATH = ENTITY_ID_PATH + "/code{?entityId,type,databricksClusterId}";

  private static final Logger LOGGER = LoggerFactory.getLogger(CodeApi.class);

  private EntityEndpointType entityType;

  public CodeApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  public void post(FeatureGroupBase featureGroup, Code code, String entityId, Code.RunType type, String browserHostName)
          throws FeatureStoreException, IOException {
    post(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
            featureGroup.getId(), code, entityId, type, browserHostName);
  }

  public void post(TrainingDataset trainingDataset, Code code, String entityId, Code.RunType type,
                   String browserHostName)
          throws FeatureStoreException, IOException {
    post(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
            trainingDataset.getId(), code, entityId, type, browserHostName);
  }

  private void post(Integer projectId, Integer featureStoreId, Integer dataSetId, Code code,
                    String entityId, Code.RunType type, String browserHostName)
          throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + CODE_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
            .set("projectId", projectId)
            .set("fsId", featureStoreId)
            .set("dataSetType", entityType.getValue())
            .set("dataSetId", dataSetId)
            .set("entityId", entityId)
            .set("type", type)
            .set("databricksClusterId", browserHostName)
            .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setEntity(hopsworksClient.buildStringEntity(code));
    hopsworksClient.handleRequest(postRequest);
  }
}
