/*
 *  Copyright (c) 2021-2023. Hopsworks AB
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
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KafkaApi {

  private static final String KAFKA_PATH = "/kafka";
  private static final String TOPIC_PATH = "/topics{/topicName}";
  private static final String SUBJECT_PATH = "/subjects{/subjectName}";
  private static final String CLUSTERINFO_PATH = "/clusterinfo{?external}";

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaApi.class);

  public Subject getTopicSubject(FeatureStoreBase featureStoreBase, String topicName)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + KAFKA_PATH + TOPIC_PATH + SUBJECT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStoreBase.getProjectId())
        .set("topicName", topicName)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), Subject.class);
  }

  public List<PartitionDetails> getTopicDetails(FeatureStoreBase featureStoreBase, String topicName)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + KAFKA_PATH + TOPIC_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStoreBase.getProjectId())
        .set("topicName", topicName)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    PartitionDetails dto = hopsworksClient.handleRequest(new HttpGet(uri), PartitionDetails.class);
    List<PartitionDetails> partitionDetails;
    if (dto.getCount() == null) {
      partitionDetails = new ArrayList<>();
      partitionDetails.add(dto);
    } else {
      partitionDetails = dto.getItems();
    }
    LOGGER.info("Received partitions: " + partitionDetails);
    return partitionDetails;
  }

  public List<String> getBrokerEndpoints(FeatureStoreBase featureStoreBase) throws FeatureStoreException, IOException {
    return getBrokerEndpoints(featureStoreBase, false);
  }

  public List<String> getBrokerEndpoints(FeatureStoreBase featureStoreBase, boolean externalListeners)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + KAFKA_PATH + CLUSTERINFO_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStoreBase.getProjectId())
        .set("external", externalListeners)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), KafkaClusterInfo.class).getBrokers();
  }
}
