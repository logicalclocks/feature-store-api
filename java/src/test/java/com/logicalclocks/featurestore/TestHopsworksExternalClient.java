package com.logicalclocks.featurestore;

import com.logicalclocks.featurestore.metadata.Credentials;
import com.logicalclocks.featurestore.metadata.HopsworksClient;
import com.logicalclocks.featurestore.metadata.HopsworksExternalClient;
import io.specto.hoverfly.junit.core.SimulationSource;
import io.specto.hoverfly.junit.dsl.HttpBodyConverter;
import io.specto.hoverfly.junit.rule.HoverflyRule;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.glassfish.jersey.internal.util.Base64;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.specto.hoverfly.junit.dsl.HoverflyDsl.service;
import static io.specto.hoverfly.junit.dsl.ResponseCreators.success;

public class TestHopsworksExternalClient {


  private String keystore = "logicalclocks";
  private String certPwd = "password";

  private Credentials credentials =
      new Credentials(Base64.encodeAsString(keystore), Base64.encodeAsString(keystore), certPwd);

  @Rule
  public HoverflyRule hoverflyRule = HoverflyRule.inSimulationMode(SimulationSource.dsl(service("test:80")
        .get("/hopsworks-api/api/project/1/credentials")
        .anyBody()
        .willReturn(success().body(HttpBodyConverter.json(credentials)))
    ));

  @Test
  public void testReadAPIKeyFromFile() throws IOException, FeatureStoreException {
    Path apiFilePath = Paths.get(System.getProperty("java.io.tmpdir"), "test.api");
    FileUtils.writeStringToFile(apiFilePath.toFile(), "hello");
    CloseableHttpClient httpClient = HttpClients.createSystem();
    HttpHost httpHost = new HttpHost("test");
    HopsworksExternalClient hopsworksExternalClient = new HopsworksExternalClient(
        httpClient, httpHost);
    String apiKey = hopsworksExternalClient.readAPIKey(null, null, apiFilePath.toString());
    Assert.assertEquals("hello", apiKey);
  }

  @Test
  public void testDownloadCredential() throws Exception {
    Project project = new Project(1);

    CloseableHttpClient httpClient = HttpClients.createSystem();
    HttpHost httpHost = new HttpHost("test");

    HopsworksExternalClient hopsworksExternalClient = new HopsworksExternalClient(
        httpClient, httpHost);

    HopsworksClient.setInstance(new HopsworksClient(hopsworksExternalClient));
    String password = hopsworksExternalClient.downloadCredentials(project, System.getProperty("java.io.tmpdir"));
    Assert.assertEquals(certPwd, password);
  }
}
