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
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.ExternalFeatureGroup;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.TimeTravelFormat;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;

public class FeatureGroupApi {

  public static final String FEATURE_GROUP_ROOT_PATH = "/featuregroups";
  public static final String FEATURE_GROUP_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgName}{?version}";
  public static final String FEATURE_GROUP_ID_PATH = FEATURE_GROUP_ROOT_PATH + "{/fgId}{?updateStatsConfig,"
      + "updateMetadata,validationType}";
  public static final String FEATURE_GROUP_COMMIT_PATH = FEATURE_GROUP_ID_PATH
      + "/commits{?filter_by,sort_by,offset,limit}";
  public static final String FEATURE_GROUP_CLEAR_PATH = FEATURE_GROUP_ID_PATH + "/clear";

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupApi.class);

  public List<FeatureGroup> getFeatureGroups(FeatureStore featureStore, String fgName)
      throws FeatureStoreException, IOException {
    FeatureGroup[] offlineFeatureGroups =
        getInternal(featureStore, fgName, null, FeatureGroup[].class);

    return Arrays.asList(offlineFeatureGroups);
  }

  public FeatureGroup getFeatureGroup(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    FeatureGroup[] offlineFeatureGroups =
        getInternal(featureStore, fgName, fgVersion, FeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    FeatureGroup resultFg = offlineFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }

  public StreamFeatureGroup getStreamFeatureGroup(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    StreamFeatureGroup[] streamFeatureGroups =
      getInternal(featureStore, fgName, fgVersion, StreamFeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    StreamFeatureGroup resultFg = streamFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }

  public List<StreamFeatureGroup> getStreamFeatureGroups(FeatureStore featureStore, String fgName)
      throws FeatureStoreException, IOException {
    StreamFeatureGroup[] streamFeatureGroups =
      getInternal(featureStore, fgName, null, StreamFeatureGroup[].class);

    return Arrays.asList(streamFeatureGroups);
  }

  public List<ExternalFeatureGroup> getExternalFeatureGroups(FeatureStore featureStore, String fgName)
      throws FeatureStoreException, IOException {
    ExternalFeatureGroup[] offlineFeatureGroups =
        getInternal(featureStore, fgName, null, ExternalFeatureGroup[].class);

    return Arrays.asList(offlineFeatureGroups);
  }

  public ExternalFeatureGroup getExternalFeatureGroup(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    ExternalFeatureGroup[] offlineFeatureGroups =
        getInternal(featureStore, fgName, fgVersion, ExternalFeatureGroup[].class);

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    ExternalFeatureGroup resultFg = offlineFeatureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }

  private <U> U getInternal(FeatureStore featureStore, String fgName, Integer fgVersion, Class<U> fgType)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_PATH;

    UriTemplate uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fgName", fgName);

    if (fgVersion != null) {
      uri.set("version", fgVersion);
    }
    String uriString = uri.expand();

    LOGGER.info("Sending metadata request: " + uriString);
    return hopsworksClient.handleRequest(new HttpGet(uriString), fgType);
  }

  public ExternalFeatureGroup save(ExternalFeatureGroup externalFeatureGroup)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    return saveInternal(externalFeatureGroup,
        hopsworksClient.buildStringEntity(externalFeatureGroup),
        ExternalFeatureGroup.class);
  }

  public FeatureGroup save(FeatureGroup featureGroup) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    return saveInternal(featureGroup, hopsworksClient.buildStringEntity(featureGroup), FeatureGroup.class);
  }

  public StreamFeatureGroup save(StreamFeatureGroup featureGroup) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    return saveInternal(featureGroup, hopsworksClient.buildStringEntity(featureGroup), StreamFeatureGroup.class);
  }

  private <U> U saveInternal(FeatureGroupBase featureGroupBase,
                             StringEntity entity, Class<U> fgType) throws FeatureStoreException, IOException {
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_ROOT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroupBase.getFeatureStore().getProjectId())
        .set("fsId", featureGroupBase.getFeatureStore().getId())
        .expand();

    HttpPost postRequest = new HttpPost(uri);
    postRequest.setEntity(entity);

    LOGGER.info("Sending metadata request: " + uri);

    return HopsworksClient.getInstance().handleRequest(postRequest, fgType);
  }

  public void delete(FeatureGroupBase featureGroupBase) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
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
    String pathTemplate = PROJECT_PATH
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
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_ID_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroup.getFeatureStore().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .set(queryParameter, value)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setEntity(hopsworksClient.buildStringEntity(featureGroup));
    return hopsworksClient.handleRequest(putRequest, fgType);
  }

  public FeatureGroupCommit featureGroupCommit(FeatureGroupBase featureGroup, FeatureGroupCommit featureGroupCommit)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_COMMIT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureGroup.getFeatureStore().getProjectId())
        .set("fsId", featureGroup.getFeatureStore().getId())
        .set("fgId", featureGroup.getId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setEntity(hopsworksClient.buildStringEntity(featureGroupCommit));
    return hopsworksClient.handleRequest(postRequest, FeatureGroupCommit.class);
  }

  public List<FeatureGroupCommit> getCommitDetails(FeatureGroupBase featureGroupBase, Long wallclockTimestamp,
                                                   Integer limit) throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = PROJECT_PATH
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

  public FeatureGroup getOrCreateFeatureGroup(FeatureStore featureStore, String name, Integer version,
                                              String description, List<String> primaryKeys, List<String> partitionKeys,
                                              String hudiPrecombineKey, boolean onlineEnabled,
                                              TimeTravelFormat timeTravelFormat,
                                              StatisticsConfig statisticsConfig, String eventTime)
      throws IOException, FeatureStoreException {


    FeatureGroup featureGroup;
    try {
      featureGroup =  getFeatureGroup(featureStore, name, version);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270009")) {
        featureGroup =  FeatureGroup.builder()
            .featureStore(featureStore)
            .name(name)
            .version(version)
            .description(description)
            .primaryKeys(primaryKeys)
            .partitionKeys(partitionKeys)
            .hudiPrecombineKey(hudiPrecombineKey)
            .onlineEnabled(onlineEnabled)
            .timeTravelFormat(timeTravelFormat)
            .statisticsConfig(statisticsConfig)
            .eventTime(eventTime)
            .build();

        featureGroup.setFeatureStore(featureStore);
      } else {
        throw e;
      }
    }

    return featureGroup;
  }

  public StreamFeatureGroup getOrCreateStreamFeatureGroup(FeatureStore featureStore, String name, Integer version,
                                                          String description, List<String> primaryKeys,
                                                          List<String> partitionKeys, String hudiPrecombineKey,
                                                          boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig,
                                                          String eventTime) throws IOException, FeatureStoreException {


    StreamFeatureGroup featureGroup;
    try {
      featureGroup =  getStreamFeatureGroup(featureStore, name, version);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270009")) {
        featureGroup =  StreamFeatureGroup.builder()
            .featureStore(featureStore)
            .name(name)
            .version(version)
            .description(description)
            .primaryKeys(primaryKeys)
            .partitionKeys(partitionKeys)
            .hudiPrecombineKey(hudiPrecombineKey)
            .onlineEnabled(onlineEnabled)
            .statisticsConfig(statisticsConfig)
            .eventTime(eventTime)
            .build();

        featureGroup.setFeatureStore(featureStore);
      } else {
        throw e;
      }
    }

    return featureGroup;
  }
}
