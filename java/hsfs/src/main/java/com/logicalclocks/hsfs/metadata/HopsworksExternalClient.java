/*
 *  Copyright (c) 2020-2023. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.SecretStore;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.ResponseHandler;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;

public class HopsworksExternalClient implements HopsworksHttpClient {

  protected static final Logger LOGGER = LoggerFactory.getLogger(HopsworksExternalClient.class.getName());
  protected static final String PARAM_NAME_SECRET_STORE = "hopsworks/role/";
  protected static final String PARAM_NAME_PARAMETER_STORE = "/hopsworks/role/";


  protected static final String MATERIAL_PASSWD = "material_passwd";
  protected static final String T_CERTIFICATE = "t_certificate";
  protected static final String K_CERTIFICATE = "k_certificate";

  protected PoolingHttpClientConnectionManager connectionPool = null;

  protected HttpHost httpHost = null;
  protected CloseableHttpClient httpClient = null;

  protected String apiKey = "";

  @Getter
  @Setter
  protected String trustStorePath;

  @Getter
  @Setter
  protected String keyStorePath;

  @Getter
  @Setter
  protected String certKey;

  public HopsworksExternalClient(CloseableHttpClient httpClient, HttpHost httpHost) {
    this.httpClient = httpClient;
    this.httpHost = httpHost;
  }

  HopsworksExternalClient(String host, int port, Region region,
                          SecretStore secretStore, boolean hostnameVerification,
                          String trustStorePath, String apiKeyFilepath, String apiKeyValue)
      throws IOException, FeatureStoreException, KeyStoreException, CertificateException,
      NoSuchAlgorithmException, KeyManagementException {

    httpHost = new HttpHost(host, port, "https");

    connectionPool = new PoolingHttpClientConnectionManager(
        createConnectionFactory(httpHost, hostnameVerification, trustStorePath));
    connectionPool.setMaxTotal(10);
    connectionPool.setDefaultMaxPerRoute(10);

    httpClient = HttpClients.custom()
        .setConnectionManager(connectionPool)
        .setKeepAliveStrategy((httpResponse, httpContext) -> 30 * 1000)
        .build();

    if (!Strings.isNullOrEmpty(apiKeyValue)) {
      this.apiKey = apiKeyValue;
    } else {
      this.apiKey = readApiKey(secretStore, region, apiKeyFilepath);
    }
  }

  protected Registry<ConnectionSocketFactory> createConnectionFactory(HttpHost httpHost, boolean hostnameVerification,
                                                                    String trustStorePath)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, KeyManagementException {

    SSLContext sslCtx = null;
    if (!Strings.isNullOrEmpty(trustStorePath)) {
      sslCtx = SSLContexts.custom()
          .loadTrustMaterial(Paths.get(trustStorePath).toFile(), null, new TrustSelfSignedStrategy())
          .build();
    } else if (!hostnameVerification) {
      // if hostnameVerification is set to false then accept also self signed certificates
      sslCtx = SSLContexts.custom()
          .loadTrustMaterial(new TrustAllStrategy())
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

  protected static String readCertKey(String materialPwd) {
    try (FileInputStream fis = new FileInputStream(materialPwd)) {
      StringBuilder sb = new StringBuilder();
      int content;
      while ((content = fis.read()) != -1) {
        sb.append((char) content);
      }
      return sb.toString();
    } catch (IOException ex) {
      LOGGER.warn("Failed to get cert password.", ex);
    }
    return null;
  }

  /**
   * Read API key.
   * We currently support 3 options:
   * - AWS Parameter store
   * - AWS Secrets manager
   * - FIle on the local file system
   *
   * @param secretStore SecretStore PARAMETER_STORE or SECRET_MANAGER
   * @param region AWS regions
   * @param apiKeyFilepath path to API key file
   * @return String
   * @throws IOException IOException
   * @throws FeatureStoreException FeatureStoreException
   */
  public String readApiKey(SecretStore secretStore, Region region, String apiKeyFilepath)
      throws IOException, FeatureStoreException {
    if (!Strings.isNullOrEmpty(apiKeyFilepath)) {
      return FileUtils.readFileToString(Paths.get(apiKeyFilepath).toFile());
    }

    switch (secretStore) {
      case PARAMETER_STORE:
        return readApiKeyParamStore(region, "api-key");
      case SECRET_MANAGER:
        return readApiKeySecretManager(region, "api-key");
      default:
        throw new FeatureStoreException("ApiKeyFilepath needs to be set for local mode");
    }
  }

  protected String readApiKeyParamStore(Region region, String secretKey) throws FeatureStoreException {
    SsmClient ssmClient = SsmClient.builder()
        .region(region)
        .build();
    String paramName = PARAM_NAME_PARAMETER_STORE + getAssumedRole() + "/type/" + secretKey;
    GetParameterRequest paramRequest = GetParameterRequest.builder()
        .name(paramName)
        .withDecryption(true)
        .build();
    GetParameterResponse parameterResponse = ssmClient.getParameter(paramRequest);
    String apiKey = parameterResponse.parameter().value();
    if (!Strings.isNullOrEmpty(apiKey)) {
      return apiKey;
    } else {
      throw new FeatureStoreException("Could not find parameter " + paramName + " in parameter store");
    }
  }

  protected String readApiKeySecretManager(Region region, String secretKey) throws FeatureStoreException, IOException {
    SecretsManagerClient secretsManagerClient = SecretsManagerClient.builder()
        .region(region)
        .build();
    String paramName = PARAM_NAME_SECRET_STORE + getAssumedRole();
    GetSecretValueRequest secretValueRequest = GetSecretValueRequest.builder()
        .secretId(paramName)
        .build();
    GetSecretValueResponse secretValueResponse = secretsManagerClient.getSecretValue(secretValueRequest);
    ObjectMapper objectMapper = new ObjectMapper();
    HashMap<String, String> secretMap = objectMapper.readValue(secretValueResponse.secretString(), HashMap.class);
    String apiKey = secretMap.get("api-key");
    if (!Strings.isNullOrEmpty(apiKey)) {
      return apiKey;
    } else {
      throw new FeatureStoreException("Could not find secret " + paramName + " in secret store");
    }
  }

  protected String getAssumedRole() throws FeatureStoreException {
    try (StsClient stsClient = StsClient.create()) {
      GetCallerIdentityResponse callerIdentityResponse = stsClient.getCallerIdentity();
      // arns for assumed roles in SageMaker follow the following schema
      // arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name
      String arn = callerIdentityResponse.arn();
      String[] arnSplits = arn.split("/");
      if (arnSplits.length != 3 || !arnSplits[0].endsWith("assumed-role")) {
        throw new FeatureStoreException("Failed to extract assumed role from arn: " + arn);
      }
      return arnSplits[1];
    }
  }

  @Override
  public <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler) throws IOException {
    LOGGER.info("Handling metadata request: " + request);
    AuthorizationHandler<T> authHandler = new AuthorizationHandler<>(responseHandler);
    request.setHeader(HttpHeaders.AUTHORIZATION, "ApiKey " + apiKey);
    try {
      return httpClient.execute(httpHost, request, authHandler);
    } catch (InternalException e) {
      // Internal exception, try one more time
      return httpClient.execute(httpHost, request, authHandler);
    }
  }
}
