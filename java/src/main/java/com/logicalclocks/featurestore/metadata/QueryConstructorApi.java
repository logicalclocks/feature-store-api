package com.logicalclocks.featurestore.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.featurestore.FeatureStore;
import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.FsQuery;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QueryConstructorApi {

  public static final String QUERY_CONSTRUCTOR_PATH = "/query";

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryConstructorApi.class);

  public String constructQuery(FeatureStore featureStore, Query query) throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH +
        FeatureStoreApi.FEATURE_STORE_SERVICE_PATH +
        QUERY_CONSTRUCTOR_PATH;

    String uri = UriTemplate .fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .expand();

    String queryJson = hopsworksClient.getObjectMapper().writeValueAsString(query);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(queryJson));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info("Sending query: " + queryJson);
    FsQuery fsQuery = hopsworksClient.handleRequest(putRequest, FsQuery.class);
    return fsQuery.getQuery().replace("\n", " ");
  }
}