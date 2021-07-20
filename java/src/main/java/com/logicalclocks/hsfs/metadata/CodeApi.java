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
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class CodeApi {

  public static final String ENTITY_ROOT_PATH = "{/entityType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  public static final String CODE_PATH = ENTITY_ID_PATH + "/code{?kernelId,type}";

  private static final Logger LOGGER = LoggerFactory.getLogger(CodeApi.class);

  private EntityEndpointType entityType;

  public CodeApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  public Code post(FeatureGroupBase featureGroup, Code code, String kernelId, String type)
          throws FeatureStoreException, IOException {
    return post(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
            featureGroup.getId(), code, kernelId, type);
  }

  public Code post(TrainingDataset trainingDataset, Code code, String kernelId, String type)
          throws FeatureStoreException, IOException {
    return post(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
            trainingDataset.getId(), code, kernelId, type);
  }

  private Code post(Integer projectId, Integer featureStoreId, Integer entityId, Code code,
                    String kernelId, String type)
          throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + CODE_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
            .set("projectId", projectId)
            .set("fsId", featureStoreId)
            .set("entityType", entityType.getValue())
            .set("entityId", entityId)
            .set("kernelId", kernelId)
            .set("type", type)
            .expand();

    String codeJson = hopsworksClient.getObjectMapper().writeValueAsString(code);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(codeJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(codeJson);

    return hopsworksClient.handleRequest(postRequest, Code.class);
  }
}
