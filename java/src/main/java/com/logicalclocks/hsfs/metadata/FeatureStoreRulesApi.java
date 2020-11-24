package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.API_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class FeatureStoreRulesApi {

  public static final String RULE_DEFINITIONS_PATH = API_PATH
      + "/rules{/name}{/predicate}{?filter_by,sort_by,offset,limit}";

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpectationsApi.class);


  public List<Rule> get() throws FeatureStoreException, IOException {
    return getRules(null, null);
  }

  public List<Rule> get(Rule.Name name) throws FeatureStoreException, IOException {
    return getRules(name, null);
  }

  public Rule get(Rule.Name name, Rule.Predicate predicate)
      throws FeatureStoreException, IOException {
    List<Rule> rules = getRules(name, predicate);
    return !rules.isEmpty() ? getRules(name, predicate).get(0) : null;
  }

  private List<Rule> getRules(Rule.Name name, Rule.Predicate predicate)
      throws FeatureStoreException, IOException {

    UriTemplate uriTemplate = UriTemplate.fromTemplate(RULE_DEFINITIONS_PATH);

    if (name != null) {
      uriTemplate.set("name", name);
    }
    if (predicate != null) {
      uriTemplate.set("predicate", predicate);
    }
    String uri = uriTemplate.expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    HopsworksClient hopsworksClient = getInstance();
    Rule rulesDto = hopsworksClient.handleRequest(getRequest, Rule.class);
    List<Rule> rules;
    if (rulesDto.getCount() == null) {
      rules = new ArrayList<>();
      rules.add(rulesDto);
    } else {
      rules = rulesDto.getItems();
    }
    LOGGER.info("Received ruleDefinitions: " + rules);
    return rules;
  }
}
