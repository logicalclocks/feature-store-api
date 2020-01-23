package com.logicalclocks.featurestore;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.featurestore.engine.SparkEngine;
import com.logicalclocks.featurestore.metadata.Query;
import com.logicalclocks.featurestore.metadata.QueryConstructorApi;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Getter @Setter
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private Integer version;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private Date created;

  @Getter @Setter
  private String creator;

  @Getter @Setter
  private FeatureStore featureStore;

  @Getter @Setter
  private List<Feature> features;

  // API
  private QueryConstructorApi queryConstructorApi;

  public FeatureGroup(String name, Integer version, String description, Date created,
                      String creator)
      throws FeatureStoreException {
    this.name = name;
    this.version = version;
    this.description = description;
    this.created = created;
    this.creator = creator;

    this.queryConstructorApi = new QueryConstructorApi();
  }

  public FeatureGroup() throws FeatureStoreException {
    this.queryConstructorApi = new QueryConstructorApi();
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return selectAll().read();
  }

  public Object head(int numRows) throws FeatureStoreException, IOException {
    return selectAll().head(numRows);
  }

  public Query select(List<Feature> features) throws FeatureStoreException, IOException {
    return new Query(this, features);
  }

  public Query selectAll() throws FeatureStoreException, IOException {
    return new Query(this, getFeatures());
  }
}
