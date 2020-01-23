package com.logicalclocks.featurestore.metadata;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.featurestore.FeatureStoreException;
import lombok.Getter;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;

public class HopsworksClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(HopsworksClient.class.getName());

  private final static String DOMAIN_CA_TRUSTSTORE = "hopsworks.domain.truststore";
  private final static String TOKEN_PATH = "token.jwt";

  public final static String API_PATH = "/hopsworks-api/api";
  public final static String PROJECT_PATH = API_PATH + "/project{/projectId}";

  private static HopsworksClient hopsworksClientInstance = null;

  public static HopsworksClient getInstance() throws FeatureStoreException {
    if (hopsworksClientInstance == null) {
      setupHopsworksClient();
    }

    return hopsworksClientInstance;
  }

  private synchronized static void setupHopsworksClient() throws FeatureStoreException {
    if (hopsworksClientInstance != null) {
      return;
    }
    try {
      hopsworksClientInstance = new HopsworksClient();
    } catch (Exception e) {
      throw new FeatureStoreException("Could not setup Hopsworks client", e);
    }
  }

  private PoolingHttpClientConnectionManager connectionPool = null;
  private HttpHost httpHost = null;
  private CloseableHttpClient httpClient = null;

  private String hopsworksEndpoint = "";
  private String jwt = "";

  @Getter
  private ObjectMapper objectMapper;

  private HopsworksClient() throws IOException, KeyStoreException, CertificateException,
      NoSuchAlgorithmException, KeyManagementException, FeatureStoreException {

    Properties systemProperties = System.getProperties();
    hopsworksEndpoint = systemProperties.getProperty("hopsworks.restendpoint");
    httpHost = HttpHost.create(hopsworksEndpoint);

    jwt = readContainerJwt();

    connectionPool = new PoolingHttpClientConnectionManager(createConnectionFactory());
    connectionPool.setMaxTotal(10);
    connectionPool.setDefaultMaxPerRoute(10);

    httpClient = HttpClients.custom()
          .setConnectionManager(connectionPool)
          .setKeepAliveStrategy((httpResponse, httpContext) -> 30 * 1000)
          .build();

    objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
  }

  private static class HopsworksHostnameVerifier implements HostnameVerifier {

    private boolean insecure = false;
    private String hopsworksHost = "";

    public HopsworksHostnameVerifier(boolean insecure, String restEndpoint) {
      this.insecure = insecure;
      this.hopsworksHost = restEndpoint.split(":")[0];
    }

    @Override
    public boolean verify(String string, SSLSession ssls) {
      return true;
      //LOGGER.info("Verifying: " + string);
      //LOGGER.info("against: " + hopsworksHost);
      //return insecure || string.equals(hopsworksHost);
    }
  }

  private Registry<ConnectionSocketFactory> createConnectionFactory()
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, KeyManagementException {
    Properties systemProperties = System.getProperties();

    Path trustStorePath = Paths.get(systemProperties.getProperty(DOMAIN_CA_TRUSTSTORE));
    LOGGER.info("Trust store path: " + trustStorePath);
    SSLContext sslCtx = SSLContexts.custom()
        .loadTrustMaterial(trustStorePath.toFile(), null, new TrustSelfSignedStrategy())
        .build();

    boolean insecure = Boolean.parseBoolean(systemProperties.getProperty("hopsutil.insecure"));

    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslCtx,
        new HopsworksHostnameVerifier(insecure, hopsworksEndpoint));

    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("https", sslsf)
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .build();
  }

  private String readContainerJwt() throws FeatureStoreException {
    String jwt = null;
    try (FileChannel fc = FileChannel.open(Paths.get(TOKEN_PATH), StandardOpenOption.READ)) {
      FileLock fileLock = fc.tryLock(0, Long.MAX_VALUE, true);
      try {
        short numRetries = 5;
        short retries = 0;
        while (fileLock == null && retries < numRetries) {
          LOGGER.debug("Waiting for lock on jwt file at:" + TOKEN_PATH);
          Thread.sleep(1000);
          fileLock = fc.tryLock(0, Long.MAX_VALUE, true);
          retries++;
        }
        //If could not acquire lock in reasonable time, throw exception
        if (fileLock == null) {
          throw new FeatureStoreException("Could not read jwt token from local container, possibly another process has" +
            " acquired the lock");
        }
        ByteBuffer buf = ByteBuffer.allocateDirect(512);
        fc.read(buf);
        buf.flip();
        jwt = StandardCharsets.UTF_8.decode(buf).toString();
      } catch (InterruptedException e) {
        LOGGER.warn("JWT waiting thread was interrupted.", e);
      } finally {
        if (fileLock != null) {
          fileLock.release();
        }
      }
    } catch (IOException e) {
      throw new FeatureStoreException("Could not read jwt token from local container." + e.getMessage(), e);
    }
    return jwt;
  }

  private static class AuthorizationHandler<T> implements ResponseHandler<T> {

    private ResponseHandler<T> originalResponseHandler;

    AuthorizationHandler(ResponseHandler<T> originalResponseHandler) {
      this.originalResponseHandler = originalResponseHandler;
    }

    @Override
    public T handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
        throw new UnauthorizedException();
      } else if (response.getStatusLine().getStatusCode() / 100 == 4) {
        throw new IOException("Error: " + response.getStatusLine().getStatusCode() +
            EntityUtils.toString(response.getEntity(), Charset.defaultCharset()));
      } else if (response.getStatusLine().getStatusCode() / 100 == 5) {
        // TODO(fabio): Propagate http error upstream
        throw new InternalException();
      }

      return originalResponseHandler.handleResponse(response);
    }
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

  private static class UnauthorizedException extends ClientProtocolException {}
  private static class InternalException extends ClientProtocolException {}

  public <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler)
      throws IOException, FeatureStoreException {
    LOGGER.debug("Handling metadata request: " + request);
    AuthorizationHandler<T> authHandler = new AuthorizationHandler<>(responseHandler);
    request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + jwt);
    try {
      return httpClient.execute(httpHost, request, authHandler);
    } catch (UnauthorizedException e) {
      // re-read the jwt and try one more time
      readContainerJwt();
      request.setHeader(HttpHeaders.AUTHORIZATION, jwt);
      return httpClient.execute(httpHost, request, responseHandler);
    } catch (InternalException e) {
      // Internal exception, try one more time
      return httpClient.execute(httpHost, request, responseHandler);
    }
  }

  public <T> T handleRequest(HttpRequest request, Class<T> cls) throws IOException, FeatureStoreException {
    return handleRequest(request, new BaseHandler<>(cls, objectMapper));
  }
}
