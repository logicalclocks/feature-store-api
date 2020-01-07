package com.logicalclocks.featurestore.metadata;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;

public class HopsworksClient {

  private final static String DOMAIN_CA_TRUSTSTORE = "hopsworks.domain.truststore";
  private final static String TOKEN_PATH = "token.jwt";

  private static HopsworksClient hopsworksClientInstance = null;

  public static HopsworksClient getInstance() {
    if (hopsworksClientInstance == null) {
      setupHopsworksClient();
    }

    return hopsworksClientInstance;
  }

  private synchronized static void setupHopsworksClient() {
    if (hopsworksClientInstance != null) {
      return;
    }
    hopsworksClientInstance = new HopsworksClient();
  }

  private PoolingHttpClientConnectionManager connectionPool = null;
  private CloseableHttpClient httpClient = null;

  private HopsworksClient() {
    connectionPool = new PoolingHttpClientConnectionManager(createConnectionFactory());
    connectionPool.setMaxTotal(10);
    connectionPool.setDefaultMaxPerRoute(10);

    httpClient = HttpClients.custom()
          .setConnectionManager(connectionPool)
          .setKeepAliveStrategy((httpResponse, httpContext) -> 30 * 1000)
          .build();
  }

  private Registry<ConnectionSocketFactory> createConnectionFactory()
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, KeyManagementException {
    Properties systemProperties = System.getProperties();

    Path trustStorePath = Paths.get(systemProperties.getProperty(DOMAIN_CA_TRUSTSTORE));
    KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());

    try (FileInputStream trustStoreIS = new FileInputStream(trustStorePath.toFile())) {
      truststore.load(trustStoreIS, null);
    }

    SSLContext sslCtx = SSLContexts.custom()
        .loadTrustMaterial(truststore, new TrustSelfSignedStrategy())
        .build();

    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslCtx, NoopHostnameVerifier.INSTANCE);
    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("https", sslsf)
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .build();
  }

}
