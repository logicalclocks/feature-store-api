package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureGroup;
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

public class StatisticsApi {

  public static final String ENTITY_ROOT_PATH = "{/entityType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  public static final String STATISTICS_PATH = ENTITY_ID_PATH + "/statistics";

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsApi.class);

  private EntityEndpointType entityType;

  public StatisticsApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  public Statistics post(FeatureGroup featureGroup, Statistics statistics) throws FeatureStoreException, IOException {
    return post(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
      featureGroup.getId(), statistics);
  }

  public Statistics post(TrainingDataset trainingDataset, Statistics statistics)
      throws FeatureStoreException, IOException {
    return post(trainingDataset.getFeatureStore().getProjectId(), trainingDataset.getFeatureStore().getId(),
      trainingDataset.getId(), statistics);
  }

  private Statistics post(Integer projectId, Integer featurestoreId, Integer entityId, Statistics statistics)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + STATISTICS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .expand();

    String statisticsJson = hopsworksClient.getObjectMapper().writeValueAsString(statistics);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(statisticsJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(statisticsJson);

    return hopsworksClient.handleRequest(postRequest, Statistics.class);
  }

}
