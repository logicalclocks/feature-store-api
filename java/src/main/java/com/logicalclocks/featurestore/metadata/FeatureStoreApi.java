package com.logicalclocks.featurestore.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.featurestore.FeatureStore;
import com.logicalclocks.featurestore.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FeatureStoreApi {

  public static final String FEATURE_STORE_SERVICE_PATH = "/featurestores";
  public static final String FEATURE_STORE_PATH =  FEATURE_STORE_SERVICE_PATH + "{/fsId}";
  public static final String FEATURE_STORE_NAME_PATH =  FEATURE_STORE_SERVICE_PATH + "{/fsName}";

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureStoreApi.class);

  public FeatureStore get(int projectId, String name) throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FEATURE_STORE_NAME_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsName", name)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), FeatureStore.class);
  }
}
