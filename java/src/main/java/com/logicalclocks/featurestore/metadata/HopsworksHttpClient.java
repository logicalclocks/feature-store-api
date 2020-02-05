package com.logicalclocks.featurestore.metadata;

import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.Project;
import org.apache.http.HttpRequest;
import org.apache.http.client.ResponseHandler;

import java.io.IOException;

public interface HopsworksHttpClient {
  <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler)
      throws IOException, FeatureStoreException;

  String downloadCredentials(Project project, String certPath) throws IOException, FeatureStoreException;
}
