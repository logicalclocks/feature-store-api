package com.logicalclocks.featurestore.metadata;

import com.logicalclocks.featurestore.FeatureStoreException;
import jdk.nashorn.internal.runtime.regexp.joni.exception.InternalException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;

public class HopsworksInternalClient implements HopsworksHttpClient {

  public final static String REST_ENDPOINT_SYS = "hopsworks.restendpoint";

  private static final Logger LOGGER = LoggerFactory.getLogger(HopsworksInternalClient.class.getName());

  private final static String DOMAIN_CA_TRUSTSTORE = "hopsworks.domain.truststore";
  private final static String TOKEN_PATH = "token.jwt";

  private PoolingHttpClientConnectionManager connectionPool = null;

  private HttpHost httpHost = null;
  private CloseableHttpClient httpClient = null;

  private String hopsworksEndpoint = "";
  private String jwt = "";

  public HopsworksInternalClient() throws IOException, KeyStoreException, CertificateException,
      NoSuchAlgorithmException, KeyManagementException, FeatureStoreException {
    hopsworksEndpoint = System.getProperties().getProperty(REST_ENDPOINT_SYS);
    httpHost = HttpHost.create(hopsworksEndpoint);

    connectionPool = new PoolingHttpClientConnectionManager(createConnectionFactory());
    connectionPool.setMaxTotal(10);
    connectionPool.setDefaultMaxPerRoute(10);

    httpClient = HttpClients.custom()
          .setConnectionManager(connectionPool)
          .setKeepAliveStrategy((httpResponse, httpContext) -> 30 * 1000)
          .build();

    jwt = readContainerJwt();
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
      request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + jwt);
      return httpClient.execute(httpHost, request, responseHandler);
    } catch (InternalException e) {
      // Internal exception, try one more time
      return httpClient.execute(httpHost, request, responseHandler);
    }
  }
}
