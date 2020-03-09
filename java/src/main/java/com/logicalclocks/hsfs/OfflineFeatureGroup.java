package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.Query;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OfflineFeatureGroup implements FeatureGroup {
  @Getter @Setter
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private Integer version;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private FeatureStore featureStore;

  @Getter @Setter
  private List<Feature> features;

  @Getter @Setter
  private String featuregroupType = "CACHED_FEATURE_GROUP";

  @Getter @Setter
  // TODO(Fabio): Refactor Hopsworks to remove this garbage here.
  private String type = "cachedFeaturegroupDTO";

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> primaryKeys;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  private FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

  @Builder
  // TODO(Fabio): here to be consistent we should also allow people to pass strings instead of feature objects
  public OfflineFeatureGroup(FeatureStore featureStore, String name, Integer version, String description)
      throws FeatureStoreException {
    if (name == null) {
      throw new FeatureStoreException("Name is required when creating a feature group");
    }
    if (version == null) {
      throw new FeatureStoreException("Version is required when creating a feature group");
    }

    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
  }

  public OfflineFeatureGroup() {
  }

  public Query selectFeatures(List<Feature> features) throws FeatureStoreException, IOException {
    return new Query(this, features);
  }

  public Query selectAll() throws FeatureStoreException, IOException {
    return new Query(this, getFeatures());
  }

  public Dataset<Row> read() {
    return featureGroupEngine.read(this);
  }

  public void show(int numRows) {
    read().show(numRows);
  }

  public void create(Dataset<Row> featureData) throws FeatureStoreException, IOException {
    create(featureData, null);
  }

  public void create(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureGroupEngine.createFeatureGroup(this, featureData, primaryKeys, partitionKeys, writeOptions);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite) {
    insert(featureData, overwrite, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions) {
    // TODO(Fabio): Overwrite will drop the table.
    featureGroupEngine.saveDataframe(this, featureData,
        overwrite ? SaveMode.Overwrite : SaveMode.Append, writeOptions);
  }
}
