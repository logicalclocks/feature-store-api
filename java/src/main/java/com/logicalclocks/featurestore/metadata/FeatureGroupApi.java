package com.logicalclocks.featurestore.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.featurestore.FeatureGroup;
import com.logicalclocks.featurestore.FeatureStore;
import com.logicalclocks.featurestore.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

public class FeatureGroupApi {

  public static final String FEATURE_GROUP_PATH = "/featuregroups{/fgName}{?version}";

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupApi.class);

  public FeatureGroupApi() throws FeatureStoreException { }

  public FeatureGroup get(FeatureStore featureStore, String fgName, Integer fgVersion)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", featureStore.getProjectId())
        .set("fsId", featureStore.getId())
        .set("fgName", fgName)
        .set("version", fgVersion)
        .expand();

    LOGGER.info("Sending metadata request: " + uri);
    FeatureGroup[] featureGroups = hopsworksClient.handleRequest(new HttpGet(uri), response -> {
      String responseJSON = EntityUtils.toString(response.getEntity(), Charset.defaultCharset());
      return hopsworksClient.getObjectMapper().readValue(responseJSON, FeatureGroup[].class);
    });

    // There can be only one single feature group with a specific name and version in a feature store
    // There has to be one otherwise an exception would have been thrown.
    FeatureGroup resultFg = featureGroups[0];
    resultFg.setFeatureStore(featureStore);
    return resultFg;
  }
}
