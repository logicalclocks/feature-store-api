package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import lombok.NonNull;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.PROJECT_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class FeatureGroupValidationsApi {

  public static final String ENTITY_ROOT_PATH = "{/entityType}";
  public static final String ENTITY_ID_PATH = ENTITY_ROOT_PATH + "{/entityId}";
  public static final String RESULTS_PATH = ENTITY_ID_PATH + "/validations{/validation_time}{?filter_by,offset,limit}";

  private static final Logger LOGGER = LoggerFactory.getLogger(RuleApi.class);

  private final EntityEndpointType entityType;

  public FeatureGroupValidationsApi(@NonNull EntityEndpointType entityType) {
    this.entityType = entityType;
  }

  public List<FeatureGroupValidation> get(FeatureGroup featureGroup)
      throws FeatureStoreException, IOException {
    return get(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
      featureGroup.getId(), null);
  }

  public FeatureGroupValidation get(FeatureGroup featureGroup, String validationTime)
      throws FeatureStoreException, IOException {
    return get(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
      featureGroup.getId(), validationTime).get(0);
  }

  private List<FeatureGroupValidation> get(Integer projectId, Integer featurestoreId, Integer entityId,
      String validationTime)
      throws FeatureStoreException, IOException {

    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + RESULTS_PATH;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId);
    if (validationTime != null) {
      uriTemplate.set("validation_time", validationTime);
    }

    String uri = uriTemplate.expand();
    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    HopsworksClient hopsworksClient = getInstance();
    FeatureGroupValidation dto = hopsworksClient.handleRequest(getRequest, FeatureGroupValidation.class);
    List<FeatureGroupValidation> validations;
    if (dto.getCount() == null) {
      validations = new ArrayList<>();
      validations.add(dto);
    } else {
      validations = dto.getItems();
    }
    LOGGER.info("Received validations: " + validations);
    return validations;
  }


  public FeatureGroupValidation put(FeatureGroup featureGroup, FeatureGroupValidation featureGroupValidation)
      throws FeatureStoreException, IOException {
    return put(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
      featureGroup.getId(), featureGroupValidation);
  }

  private FeatureGroupValidation put(Integer projectId, Integer featurestoreId, Integer entityId,
      FeatureGroupValidation featureGroupValidation)
      throws FeatureStoreException, IOException {

    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + RESULTS_PATH;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", projectId)
        .set("fsId", featurestoreId)
        .set("entityType", entityType.getValue())
        .set("entityId", entityId)
        .expand();

    FeatureGroupValidations validations =
        FeatureGroupValidations.builder().validations(featureGroupValidation.getValidations())
        .validationTime(featureGroupValidation.getValidationTime()).build();
    String results = hopsworksClient.getObjectMapper().writeValueAsString(validations);
    HttpPut putRequest = new HttpPut(uri);
    putRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    putRequest.setEntity(new StringEntity(results));

    LOGGER.info("Sending metadata request: " + uri);
    LOGGER.info(results);

    return hopsworksClient.handleRequest(putRequest, FeatureGroupValidation.class);
  }

  public String put2(FeatureGroup featureGroup, FeatureGroupValidation featureGroupValidation)
      throws FeatureStoreException, IOException {
    return put2(featureGroup.getFeatureStore().getProjectId(), featureGroup.getFeatureStore().getId(),
      featureGroup.getId(), featureGroupValidation);
  }

  private String put2(Integer projectId, Integer featurestoreId, Integer entityId,
      FeatureGroupValidation featureGroupValidation)
      throws FeatureStoreException, IOException {

    HopsworksClient hopsworksClient = getInstance();
    String pathTemplate = PROJECT_PATH + FeatureStoreApi.FEATURE_STORE_PATH + RESULTS_PATH;

    FeatureGroupValidations validations =
        FeatureGroupValidations.builder().validations(featureGroupValidation.getValidations())
        .validationTime(featureGroupValidation.getValidationTime()).build();
    return hopsworksClient.getObjectMapper().configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false)
        .writeValueAsString(validations);
  }
}
