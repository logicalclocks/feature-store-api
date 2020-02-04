package com.logicalclocks.featurestore.metadata;

import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.Project;
import com.logicalclocks.featurestore.SecretStore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.util.Base64;
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
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class HopsworksExternalClient implements HopsworksHttpClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(HopsworksExternalClient.class.getName());
  private static final String PARAM_NAME = "/hopsworks/role/";

  private PoolingHttpClientConnectionManager connectionPool = null;

  private HttpHost httpHost = null;
  private CloseableHttpClient httpClient = null;

  private String apiKey = "";

  public HopsworksExternalClient(String host, int port, Region region,
                                 SecretStore secretStore, boolean hostnameVerification,
                                 String trustStorePath, String APIKeyFilePath)
      throws IOException, FeatureStoreException, KeyStoreException, CertificateException,
      NoSuchAlgorithmException, KeyManagementException {

    httpHost = new HttpHost(host, port);

    connectionPool = new PoolingHttpClientConnectionManager(
        createConnectionFactory(httpHost, hostnameVerification, trustStorePath));
    connectionPool.setMaxTotal(10);
    connectionPool.setDefaultMaxPerRoute(10);

    httpClient = HttpClients.custom()
          .setConnectionManager(connectionPool)
          .setKeepAliveStrategy((httpResponse, httpContext) -> 30 * 1000)
          .build();

    apiKey = readAPIKey(secretStore, region, APIKeyFilePath);
  }

  public HopsworksExternalClient(CloseableHttpClient httpClient, HttpHost httpHost) {
    this.httpClient = httpClient;
    this.httpHost = httpHost;
  }

  private Registry<ConnectionSocketFactory> createConnectionFactory(HttpHost httpHost, boolean hostnameVerification,
                                                                    String trustStorePath)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, KeyManagementException {

    SSLContext sslCtx = null;
    if (!Strings.isNullOrEmpty(trustStorePath)) {
      sslCtx = SSLContexts.custom()
          .loadTrustMaterial(Paths.get(trustStorePath).toFile(), null, new TrustSelfSignedStrategy())
          .build();
    } else {
      sslCtx = SSLContext.getDefault();
    }

    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslCtx,
        new HopsworksHostnameVerifier(hostnameVerification, httpHost.toHostString()));

    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("https", sslsf)
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .build();
  }

  /**
   * Read API key - We currently support 3 options:
   * - AWS Parameter store
   * - AWS Secrets manager
   * - FIle on the local file system
   * @param secretStore
   * @param region
   * @param APIKeyFilePath
   * @return
   * @throws IOException
   * @throws FeatureStoreException
   */
  public String readAPIKey(SecretStore secretStore, Region region, String APIKeyFilePath)
      throws IOException, FeatureStoreException {
    if (!Strings.isNullOrEmpty(APIKeyFilePath)) {
      return FileUtils.readFileToString(Paths.get(APIKeyFilePath).toFile());
    }

    switch (secretStore) {
      case PARAMETER_STORE:
        return readAPIKeyParamStore(region, "api-key");
      case SECRET_MANAGER:
        return readAPIKeySecretManager(region, "api-key");
      default:
        throw new FeatureStoreException("APIKeyFilePath needs to be set for local mode");
    }
  }

  private String readAPIKeyParamStore(Region region, String secretKey) throws FeatureStoreException {
    SsmClient ssmClient = SsmClient.builder()
        .region(region)
        .build();
    String paramName = PARAM_NAME + getAssumedRole() + "/type/" + secretKey;
    GetParameterRequest paramRequest = GetParameterRequest.builder()
        .name(paramName)
        .withDecryption(true)
       .build();
    GetParameterResponse parameterResponse = ssmClient.getParameter(paramRequest);
    return parameterResponse.getValueForField("Parameter", String.class)
        .orElseThrow(() -> new FeatureStoreException("Could not find parameter " + paramName + " in parameter store"));
  }

  private String readAPIKeySecretManager(Region region, String secretKey) throws FeatureStoreException {
    SecretsManagerClient secretsManagerClient = SecretsManagerClient.builder()
        .region(region)
        .build();
    String paramName = PARAM_NAME + getAssumedRole();
    GetSecretValueRequest secretValueRequest = GetSecretValueRequest.builder()
        .secretId(paramName)
        .build();
    GetSecretValueResponse secretValueResponse = secretsManagerClient.getSecretValue(secretValueRequest);
    return secretValueResponse.getValueForField(secretKey, String.class)
        .orElseThrow(() -> new FeatureStoreException("Could not find secret " + paramName + " in secret store"));
  }

  private String getAssumedRole() throws FeatureStoreException {
    StsClient stsClient = StsClient.create();
    GetCallerIdentityResponse callerIdentityResponse = stsClient.getCallerIdentity();
    // arns for assumed roles in SageMaker follow the following schema
    // arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name
    String arn = callerIdentityResponse.arn();
    String[] arnSplits = arn.split("/");
    if (arnSplits.length != 3 || !arnSplits[0].equals("assumed-role")) {
      throw new FeatureStoreException("Failed to extract assumed role from arn: " + arn);
    }
    return arnSplits[1];
  }

  @Override
  public <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler) throws IOException,
      FeatureStoreException {
    LOGGER.debug("Handling metadata request: " + request);
    AuthorizationHandler<T> authHandler = new AuthorizationHandler<>(responseHandler);
    request.setHeader(HttpHeaders.AUTHORIZATION, "ApiKey " + apiKey);
    try {
      return httpClient.execute(httpHost, request, authHandler);
    } catch (InternalException e) {
      // Internal exception, try one more time
      return httpClient.execute(httpHost, request, responseHandler);
    }
  }

  @Override
  public String downloadCredentials(Project project, String certPath) throws IOException, FeatureStoreException {
    LOGGER.info("Fetching certificates for the project");
    ProjectApi projectApi = new ProjectApi();
    Credentials credentials = projectApi.downloadCredentials(project);

    FileUtils.writeByteArrayToFile(Paths.get(certPath, "keyStore.jks").toFile(),
        Base64.decodeBase64(credentials.getKStore()));
    FileUtils.writeByteArrayToFile(Paths.get(certPath, "trustStore.jks").toFile(),
        Base64.decodeBase64(credentials.getTStore()));
    return credentials.getPassword();
  }
}
