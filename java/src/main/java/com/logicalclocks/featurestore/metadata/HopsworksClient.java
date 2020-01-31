package com.logicalclocks.featurestore.metadata;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.SecretStore;
import lombok.Getter;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;
import org.apache.parquet.Strings;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class HopsworksClient {

  public final static String API_PATH = "/hopsworks-api/api";
  public final static String PROJECT_PATH = API_PATH + "/project{/projectId}";

  private static HopsworksClient hopsworksClientInstance = null;

  public static HopsworksClient getInstance() throws FeatureStoreException {
    return hopsworksClientInstance;
  }

  public synchronized static void setupHopsworksClient(String host, int port, String project, Region region,
                                                       SecretStore secretStore, boolean hostnameVerification,
                                                       String trustStorePath, String certPath, String APIKeyFilePath)
      throws FeatureStoreException {
    if (hopsworksClientInstance != null) {
      return;
    }

    HopsworksHttpClient hopsworksHttpClient = null;
    try {
      if (System.getProperties().contains(HopsworksInternalClient.REST_ENDPOINT_SYS)) {
        hopsworksHttpClient = new HopsworksInternalClient();
      } else {
        hopsworksHttpClient = new HopsworksExternalClient(host, port, project, region,
            secretStore, hostnameVerification, trustStorePath, certPath, APIKeyFilePath);
      }
    } catch (Exception e) {
      throw new FeatureStoreException("Could not setup Hopsworks client", e);
    }

    hopsworksClientInstance = new HopsworksClient(hopsworksHttpClient);
  }

  private HopsworksHttpClient hopsworksHttpClient;

  @Getter
  private ObjectMapper objectMapper;

  private HopsworksClient(HopsworksHttpClient hopsworksHttpClient) {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);

    this.hopsworksHttpClient = hopsworksHttpClient;
  }

  private static class BaseHandler<T> implements ResponseHandler<T> {

    private Class<T> cls;
    private ObjectMapper objectMapper;

    public BaseHandler(Class<T> cls, ObjectMapper objectMapper) {
      this.cls = cls;
      this.objectMapper = objectMapper;
    }

    @Override
    public T handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
      String responseJSON = EntityUtils.toString(response.getEntity(), Charset.defaultCharset());
      return objectMapper.readValue(responseJSON, cls);
    }
  }

  public <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler)
      throws IOException, FeatureStoreException {
    return hopsworksHttpClient.handleRequest(request, responseHandler);
  }

  public <T> T handleRequest(HttpRequest request, Class<T> cls) throws IOException, FeatureStoreException {
    return hopsworksHttpClient.handleRequest(request, new BaseHandler<>(cls, objectMapper));
  }
}
