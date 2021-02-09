package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class KafkaApi {

  private static final String KAFKA_PATH = "/kafka";
  private static final String TOPIC_PATH = "/topics{/topicName}";
  private static final String SUBJECT_PATH = "/subjects{/subjectName}";
  private static final String CLUSTERINFO_PATH = "/clusterinfo";

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaApi.class);

  public Subject getTopicSubject(FeatureStore featureStore, String topicName)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + KAFKA_PATH + TOPIC_PATH + SUBJECT_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("topicName", topicName)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), Subject.class);
  }

  public List<String> getBrokerEndpoints(FeatureStore featureStore) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + KAFKA_PATH + CLUSTERINFO_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), KafkaClusterInfo.class).getBrokers();
  }
}
