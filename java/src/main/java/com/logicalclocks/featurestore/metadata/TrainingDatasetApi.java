package com.logicalclocks.featurestore.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.TrainingDataset;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TrainingDatasetApi {

  private static final String TRAINING_DATASET_PATH = "/trainingdatasets";

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupApi.class);

  public TrainingDataset createTrainingDataset(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + TRAINING_DATASET_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", trainingDataset.getFeatureStore().getProjectId())
        .set("fsId", trainingDataset.getFeatureStore().getId())
        .expand();

    String trainingDatasetJson = hopsworksClient.getObjectMapper().writeValueAsString(trainingDataset);
    HttpPost postRequest = new HttpPost(uri);
    postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    postRequest.setEntity(new StringEntity(trainingDatasetJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(trainingDatasetJson);
    return hopsworksClient.handleRequest(postRequest, TrainingDataset.class);
  }
}
