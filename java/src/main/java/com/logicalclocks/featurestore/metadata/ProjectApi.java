package com.logicalclocks.featurestore.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.featurestore.FeatureStoreException;
import com.logicalclocks.featurestore.Project;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProjectApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectApi.class);

  private static final String PROJECT_INFO_PATH = "/project/getProjectInfo{/projectName}";
  private static final String CREDENTIALS_PATH = "/project{/projectId}/credentials";

  public Project get(String name) throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(HopsworksClient.API_PATH + PROJECT_INFO_PATH)
        .set("projectName", name)
        .expand();
    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), Project.class);
  }

  public Credentials downloadCredentials(Project project) throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String uri = UriTemplate.fromTemplate(HopsworksClient.API_PATH + CREDENTIALS_PATH)
        .set("projectId", project.getProjectId())
        .expand();
    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), Credentials.class);
  }
}
