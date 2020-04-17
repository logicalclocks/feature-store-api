package com.logicalclocks.featurestore.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.Project;
import com.logicalclocks.featurestore.SecretStore;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.nio.charset.Charset;

public class HopsworksClient {

  public final static String API_PATH = "/hopsworks-api/api";
  public final static String PROJECT_PATH = API_PATH + "/project{/projectId}";

  private static HopsworksClient hopsworksClientInstance = null;
  private static final Logger LOGGER = LoggerFactory.getLogger(HopsworksClient.class);

  private String certPwd = "";

  public static HopsworksClient getInstance() throws FeatureStoreException {
    if (hopsworksClientInstance == null) {
      throw new FeatureStoreException("Client not connected. Please establish a Hopsworks connection first");
    }
    return hopsworksClientInstance;
  }

  // For testing
  public static void setInstance(HopsworksClient instance) {
    hopsworksClientInstance = instance;
  }

  public synchronized static HopsworksClient setupHopsworksClient(String host, int port, Region region,
                                                       SecretStore secretStore, boolean hostnameVerification,
                                                       String trustStorePath, String APIKeyFilePath)
      throws FeatureStoreException {
    if (hopsworksClientInstance != null) {
      return hopsworksClientInstance;
    }

    HopsworksHttpClient hopsworksHttpClient = null;
    try {
      if (System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS)) {
        hopsworksHttpClient = new HopsworksInternalClient();
      } else {
        hopsworksHttpClient = new HopsworksExternalClient(host, port, region,
            secretStore, hostnameVerification, trustStorePath, APIKeyFilePath);
      }
    } catch (Exception e) {
      throw new FeatureStoreException("Could not setup Hopsworks client", e);
    }

    hopsworksClientInstance = new HopsworksClient(hopsworksHttpClient);
    return hopsworksClientInstance;
  }

  private HopsworksHttpClient hopsworksHttpClient;

  @Getter
  private ObjectMapper objectMapper;

  @VisibleForTesting
  public HopsworksClient(HopsworksHttpClient hopsworksHttpClient) {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

    this.hopsworksHttpClient = hopsworksHttpClient;
  }

  @AllArgsConstructor
  @NoArgsConstructor
  private static class HopsworksErrorClass {
    @Getter @Setter
    private Integer errorCode;
    @Getter @Setter
    private String usrMsg;
    @Getter @Setter
    private String devMsg;

    @Override
    public String toString() {
      return "errorCode=" + errorCode + ", usrMsg='" + usrMsg + '\'' + ", devMsg='" + devMsg + '\'';
    }
  }

  private static class AuthorizationException extends ClientProtocolException {}

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
      if (response.getStatusLine().getStatusCode() / 100 == 2) {
        return objectMapper.readValue(responseJSON, cls);
      } else if (response.getStatusLine().getStatusCode() == 401) {
        throw new AuthorizationException();
      } else {
        HopsworksErrorClass error = objectMapper.readValue(responseJSON, HopsworksErrorClass.class);
        LOGGER.info("Request error: " + response.getStatusLine().getStatusCode() + " " + error);
        throw new ClientProtocolException("Request error: " + response.getStatusLine().getStatusCode() + " " + error);
      }
    }
  }

  public <T> T handleRequest(HttpRequest request, Class<T> cls) throws IOException, FeatureStoreException {
    try {
      return hopsworksHttpClient.handleRequest(request, new BaseHandler<>(cls, objectMapper));
    } catch (AuthorizationException ex) {
      // In case of authorization exception we should try to reload the JWT from the FS. Maybe it was expired and
      // it has been renewed
      hopsworksHttpClient.refreshJWT();
      return hopsworksHttpClient.handleRequest(request, new BaseHandler<>(cls, objectMapper));
    }
  }

  public void downloadCredentials(Project project, String certPath) throws IOException, FeatureStoreException {
    certPwd = hopsworksHttpClient.downloadCredentials(project, certPath);
  }
}
