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
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.engine.DataValidationEngine;
import lombok.NonNull;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class RuleApi {

  public static final String ENTITY_ROOT_PATH = "{/entityType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  public static final String RULES_PATH =
      ENTITY_ID_PATH + "/rules{/name}{/predicate}{/feature}{?engine,filter_by,sort_by,offset,limit}";

  private static final Logger LOGGER = LoggerFactory.getLogger(RuleApi.class);

  private EntityEndpointType entityType;

  public RuleApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  public Rule put(FeatureGroup featureGroup, Rule rule) throws FeatureStoreException, IOException {
    return put(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
      featureGroup.getId(), rule);
  }

  private Rule put(Integer projectId, Integer featurestoreId, Integer entityId, Rule rule)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + RULES_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .set("name", rule.getName())
        .set("predicate", rule.getPredicate())
        .set("feature", rule.getFeature())
        .expand();

    String ruleConfigurationJson = hopsworksClient.getObjectMapper().writeValueAsString(rule.getRuleConfiguration());
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(ruleConfigurationJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(ruleConfigurationJson);

    return hopsworksClient.handleRequest(putRequest, Rule.class);
  }

  public List<Rule> get(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    return get(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
      featureGroup.getId(), null, null, null, null);
  }

  public Rule get(FeatureGroup featureGroup, Rule.Name name, Rule.Predicate predicate,
      String feature) throws FeatureStoreException, IOException {
    List<Rule> rules = get(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
        featureGroup.getId(), name, predicate, feature, null);
    return !rules.isEmpty() ? rules.get(0) : null;
  }

  private List<Rule> get(Integer projectId, Integer featurestoreId, Integer entityId, Rule.Name name,
      Rule.Predicate predicate, String feature, DataValidationEngine.Engine engine)
      throws FeatureStoreException, IOException {
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + RULES_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId);

    if (name != null) {
      uriTemplate.set("name", name);
    }
    if (predicate != null) {
      uriTemplate.set("predicate", predicate);
    }
    if (feature != null) {
      uriTemplate.set("feature", feature);
    }

    if (engine != null) {
      uriTemplate.set("engine", engine);
    }

    String uri = uriTemplate.expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    HopsworksClient hopsworksClient = getInstance();
    Rule ruleDto = hopsworksClient.handleRequest(getRequest, Rule.class);
    List<Rule> rules;
    if (ruleDto.getCount() == null) {
      rules = new ArrayList<>();
      rules.add(ruleDto);
    } else {
      rules = ruleDto.getItems();
    }
    LOGGER.info("Received rules: " + rules);
    return rules;
  }

}
