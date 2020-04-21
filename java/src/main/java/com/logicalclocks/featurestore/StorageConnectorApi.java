package com.logicalclocks.featurestore;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.featurestore.metadata.FeatureStoreApi;
import com.logicalclocks.featurestore.metadata.HopsworksClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class StorageConnectorApi {

  private static final String CONNECTOR_PATH = "/storageconnectors{/connType}";
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageConnectorApi.class);

  public StorageConnector getByNameAndType(FeatureStore featureStore, String name, StorageConnectorType type)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + CONNECTOR_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("connType", type)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    StorageConnector[] storageConnectors = hopsworksClient.handleRequest(new HttpGet(uri), StorageConnector[].class);

    return Arrays.stream(storageConnectors).filter(s -> s.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new FeatureStoreException("Could not find storage connector " + name + " with type " + type));
  }

}
