package com.logicalclocks.featurestore;

import com.google.common.base.Strings;
import com.logicalclocks.featurestore.metadata.FeatureStoreApi;
import com.logicalclocks.featurestore.metadata.HopsworksClient;
import com.logicalclocks.featurestore.metadata.HopsworksInternalClient;
import com.logicalclocks.featurestore.metadata.ProjectApi;
import com.logicalclocks.featurestore.util.Constants;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.Closeable;
import java.io.IOException;

public class HopsworksConnection implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HopsworksConnection.class);

  @Getter
  private String host;

  @Getter
  private int port;

  @Getter
  private String project;

  @Getter
  private Region region;

  @Getter
  private SecretStore secretStore;

  @Getter
  private boolean hostnameVerification;

  @Getter
  private String trustStorePath;

  @Getter
  private String certPath;

  @Getter
  private String APIKeyFilePath;


  private FeatureStoreApi featureStoreApi = new FeatureStoreApi();
  private ProjectApi projectApi = new ProjectApi();

  private Project projectObj;

  @Builder
  public HopsworksConnection(String host, int port, String project, Region region, SecretStore secretStore,
                             boolean hostnameVerification, String trustStorePath,
                             String certPath, String APIKeyFilePath) throws IOException, FeatureStoreException {
    this.host = host;
    this.port = port;
    this.project = project;
    this.region = region;
    this.secretStore = secretStore;
    this.hostnameVerification = hostnameVerification;
    this.trustStorePath = trustStorePath;
    this.certPath = certPath;
    this.APIKeyFilePath = APIKeyFilePath;

    HopsworksClient hopsworksClient = HopsworksClient.setupHopsworksClient(host, port, region, secretStore,
        hostnameVerification, trustStorePath, APIKeyFilePath);
    projectObj = getProject();
    hopsworksClient.downloadCredentials(projectObj, certPath);
  }

  /**
   * Retrieve the project feature store
   * @return
   * @throws IOException
   * @throws FeatureStoreException
   */
  public FeatureStore getFeatureStore() throws IOException, FeatureStoreException{
    return getFeatureStore(project + Constants.FEATURESTORE_SUFFIX);
  }

  /**
   * Retrieve a feature store based on name. The feature store needs to be shared with
   * the connection's project.
   * @param name
   * @return
   * @throws IOException
   * @throws FeatureStoreException
   */
  public FeatureStore getFeatureStore(String name) throws IOException, FeatureStoreException {
    return featureStoreApi.get(projectObj.getProjectId(), name);
  }

  /**
   * Close the connection and clean up the certificates.
   */
  public void close() {
    // Close the client
  }

  private Project getProject() throws IOException, FeatureStoreException {
    if (Strings.isNullOrEmpty(project)) {
      // User didn't specify a project in the connection construction. Assume they are running
      // from within Hopsworks and the project name is available a system property
      project = System.getProperty(Constants.PROJECTNAME_ENV);
    }
    LOGGER.info("Getting information for project name: " + project);
    return projectApi.get(project);
  }

  /**
   * In case of connection from outside Hopsworks, fetch the certificates for the project
   */
  private void localizeCertificates() {
    if (System.getProperties().contains(HopsworksInternalClient.REST_ENDPOINT_SYS)) {
      return;
    }
  }
}
