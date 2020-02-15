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
import java.util.stream.Collectors;

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

  public void show(int numRows) throws FeatureStoreException, IOException {
    selectAll().show(numRows);
  }

  public Query select(List<String> features) throws FeatureStoreException, IOException {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList  = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  public Query selectFeatures(List<Feature> features) throws FeatureStoreException, IOException {
    return new Query(this, features);
  }

  public Query selectAll() throws FeatureStoreException, IOException {
    return new Query(this, getFeatures());
  }
}
