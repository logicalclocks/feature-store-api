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
import com.logicalclocks.base.DeltaStreamerJobConf;
import com.logicalclocks.base.Feature;
import com.logicalclocks.base.FeatureGroupCommit;
import com.logicalclocks.base.FeatureStoreBase;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.JobConfiguration;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureGroupApi {

  public static final String FEATURE_GROUP_ROOT_PATH = "/featuregroups";
  public static final String FEATURE_GROUP_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgName}{?version}";
  public static final String FEATURE_GROUP_ID_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgId}{?updateStatsConfig,"
      + "updateMetadata,validationType}";
  public static final String FEATURE_GROUP_COMMIT_PATH = FEATURE_GROUP_ID_PATH
      + "/commits{?filter_by,sort_by,offset,limit}";
  public static final String FEATURE_GROUP_CLEAR_PATH = FEATURE_GROUP_ID_PATH + "/clear";

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupApi.class);

  public <U> U getInternal(FeatureStoreBase featureStoreBase, String fgName, Integer fgVersion, Class<U> fgType)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_PATH;

    UriTemplate uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStoreBase.getProjectId())
        .set("fsId", featureStoreBase.getId())
        .set("fgName", fgName);

    if (fgVersion != null) {
      uri.set("version", fgVersion);
    }
    String uriString = uri.expand();

    LOGGER.info("Sending metadata request: " + uriString);
    return hopsworksClient.handleRequest(new HttpGet(uriString), fgType);
  }

  public <U extends FeatureGroupBase> FeatureGroupBase save(FeatureGroupBase featureGroup, Class<U> fgType)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String featureGroupJson = hopsworksClient.getObjectMapper().writeValueAsString(featureGroup);

    return saveInternal(featureGroup, new StringEntity(featureGroupJson), fgType);
  }

  public <U extends FeatureGroupBase> FeatureGroupBase saveInternal(FeatureGroupBase featureGroupBase,
                             StringEntity entity, Class<U> fgType) throws FeatureStoreException, IOException {
    String pathTemplate = HopsworksClient.PROJECT_PATH
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

  public void delete(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroupBase.getFeatureStore().getProjectId())
        .set("fsId", featureGroupBase.getFeatureStore().getId())
        .set("fgId", featureGroupBase.getId())
        .expand();

    HttpDelete deleteRequest = new HttpDelete(uri);

    LOGGER.info("Sending metadata request: " + uri);
    hopsworksClient.handleRequest(deleteRequest);
  }

  public void deleteContent(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_CLEAR_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroup.getFeatureStore().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpPost postRequest = new HttpPost(uri);
    hopsworksClient.handleRequest(postRequest);
  }

  public <T extends FeatureGroupBase> T updateMetadata(FeatureGroupBase featureGroup, String queryParameter,
                                                       Class<T> fgType)
      throws FeatureStoreException, IOException {
    return updateMetadata(featureGroup, queryParameter, true, fgType);
  }

  public <T extends FeatureGroupBase> T updateMetadata(FeatureGroupBase featureGroup, String queryParameter,
                                                       Object value, Class<T> fgType)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroup.getFeatureStore().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .set(queryParameter, value)
        .expand();

    String featureGroupJson = hopsworksClient.getObjectMapper().writeValueAsString(featureGroup);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(featureGroupJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(featureGroupJson);

    return hopsworksClient.handleRequest(putRequest, fgType);
  }

  public FeatureGroupCommit featureGroupCommit(FeatureGroupBase featureGroup, FeatureGroupCommit featureGroupCommit)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_COMMIT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroup.getFeatureStore().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .expand();

    String featureGroupCommitJson = hopsworksClient.getObjectMapper().writeValueAsString(featureGroupCommit);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(featureGroupCommitJson));

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(postRequest, FeatureGroupCommit.class);
  }

  public List<FeatureGroupCommit> getCommitDetails(FeatureGroupBase featureGroupBase, Long wallclockTimestamp,
                                                   Integer limit) throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_COMMIT_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroupBase.getFeatureStore().getProjectId())
        .set("fsId", featureGroupBase.getFeatureStore().getId())
        .set("fgId", featureGroupBase.getId())
        .set("sort_by", "committed_on:desc")
        .set("offset", 0)
        .set("limit", limit);

    if (wallclockTimestamp != null) {
      uriTemplate.set("filter_by", "commited_on_ltoeq:" + wallclockTimestamp);
    }

    String uri = uriTemplate.expand();

    LOGGER.info("Sending metadata request: " + uri);
    FeatureGroupCommit featureGroupCommit = hopsworksClient.handleRequest(new HttpGet(uri), FeatureGroupCommit.class);
    return featureGroupCommit.getItems();
  }

  public <U extends FeatureGroupBase> FeatureGroupBase saveFeatureGroupMetaData(
      FeatureGroupBase featureGroup, List<String> partitionKeys,
      String hudiPrecombineKey, Map<String, String> writeOptions,
      JobConfiguration  jobConfiguration, Class<U> fgType) throws FeatureStoreException, IOException {

    LOGGER.info("Featuregroup features: " + featureGroup.getFeatures());

    /* set primary features */
    if (featureGroup.getPrimaryKeys() != null) {
      featureGroup.getPrimaryKeys().forEach(pk ->
          featureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPrimary(true);
            }
          }));
    }

    /* set partition key features */
    if (partitionKeys != null) {
      partitionKeys.forEach(pk ->
          featureGroup.getFeatures().forEach(f -> {
            if (f.getName().equals(pk)) {
              f.setPartition(true);
            }
          }));
    }

    /* set hudi precombine key name */
    if (hudiPrecombineKey != null) {
      featureGroup.getFeatures().forEach(f -> {
        if (f.getName().equals(hudiPrecombineKey)) {
          f.setHudiPrecombineKey(true);
        }
      });
    }

    // set write options for delta streamer job
    if (writeOptions != null) {
      // set write options for delta streamer job
      DeltaStreamerJobConf deltaStreamerJobConf = new DeltaStreamerJobConf();
      deltaStreamerJobConf.setWriteOptions(writeOptions != null ? writeOptions.entrySet().stream()
          .map(e -> new Option(e.getKey(), e.getValue()))
          .collect(Collectors.toList())
          : null);
      deltaStreamerJobConf.setSparkJobConfiguration(jobConfiguration);

      featureGroup.setDeltaStreamerJobConf(deltaStreamerJobConf);
    }

    // Send Hopsworks the request to create a new feature group
    FeatureGroupBase apiFG = save(featureGroup, fgType);

    if (featureGroup.getVersion() == null) {
      LOGGER.info("VersionWarning: No version provided for creating feature group `" + featureGroup.getName()
          + "`, incremented version to `" + apiFG.getVersion() + "`.");
    }

    // Update the original object - Hopsworks returns the incremented version
    featureGroup.setId(apiFG.getId());
    featureGroup.setVersion(apiFG.getVersion());
    featureGroup.setLocation(apiFG.getLocation());
    featureGroup.setStatisticsConfig(apiFG.getStatisticsConfig());
    featureGroup.setOnlineTopicName(apiFG.getOnlineTopicName());

    /* if hudi precombine key was not provided and TimeTravelFormat is HUDI, retrieve from backend and set */
    if (hudiPrecombineKey == null) {
      List<Feature> features = apiFG.getFeatures();
      featureGroup.setFeatures(features);
    }

    return featureGroup;
  }
}
