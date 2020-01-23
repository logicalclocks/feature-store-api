package com.logicalclocks.featurestore;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.featurestore.metadata.Query;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Date;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroup {

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

  public FeatureGroup(String name, Integer version, String description, Date created, String creator) {
    this.name = name;
    this.version = version;
    this.description = description;
    this.created = created;
    this.creator = creator;
  }

  public FeatureGroup() {
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
