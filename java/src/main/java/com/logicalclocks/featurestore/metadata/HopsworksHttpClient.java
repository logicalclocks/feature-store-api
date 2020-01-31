package com.logicalclocks.featurestore.metadata;

import com.logicalclocks.featurestore.FeatureStoreException;
import org.apache.http.HttpRequest;
import org.apache.http.client.ResponseHandler;

import java.io.IOException;

public interface HopsworksHttpClient {
  <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler)
      throws IOException, FeatureStoreException;
}
