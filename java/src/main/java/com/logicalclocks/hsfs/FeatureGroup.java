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
import java.util.Date;
import java.util.List;
import java.util.Map;
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
  private FeatureStore featureStore;

  @Getter @Setter
  private List<Feature> features;

  @Getter
  private Date created;

  @Getter
  private String creator;

  @Getter @Setter
  private Storage defaultStorage;

  @Getter @Setter
  private Boolean onlineEnabled;

  @Getter @Setter
  private String type = "cachedFeaturegroupDTO";

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> primaryKeys;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  private FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

  @Builder
  public FeatureGroup(FeatureStore featureStore, String name, Integer version, String description,
                      List<String> primaryKeys, List<String> partitionKeys,
                      boolean onlineEnabled, Storage defaultStorage, List<Feature> features)
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
    this.primaryKeys = primaryKeys;
    this.partitionKeys = partitionKeys;
    this.onlineEnabled = onlineEnabled;
    this.defaultStorage = defaultStorage != null ? defaultStorage : Storage.OFFLINE;
    this.features = features;
  }

  public FeatureGroup() {
  }

  public Query selectFeatures(List<Feature> features) throws FeatureStoreException, IOException {
    return new Query(this, features);
  }

  public Query selectAll() throws FeatureStoreException, IOException {
    return new Query(this, getFeatures());
  }

  public Query select(List<String> features) throws FeatureStoreException, IOException {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList  = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    return read(this.defaultStorage);
  }

  public Dataset<Row> read(Storage storage) throws FeatureStoreException, IOException {
    return selectAll().read(storage);
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    show(numRows, defaultStorage);
  }

  public void show(int numRows, Storage storage) throws FeatureStoreException, IOException {
    read(storage).show(numRows);
  }

  public void save(Dataset<Row> featureData) throws FeatureStoreException, IOException {
    save(featureData, defaultStorage, null);
  }

  public void save(Dataset<Row> featureData, Storage storage) throws FeatureStoreException, IOException {
    save(featureData, storage, null);
  }

  public void save(Dataset<Row> featureData, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    save(featureData, defaultStorage, writeOptions);
  }

  public void save(Dataset<Row> featureData, Storage storage, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureGroupEngine.saveFeatureGroup(this, featureData, primaryKeys, partitionKeys, storage, writeOptions);
  }

  public void insert(Dataset<Row> featureData, Storage storage) throws IOException, FeatureStoreException {
    insert(featureData, storage, false, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite) throws IOException, FeatureStoreException {
    insert(featureData, overwrite, null);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite)
      throws IOException, FeatureStoreException {
    insert(featureData, storage, overwrite, null);
  }

  public void insert(Dataset<Row> featureData, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    insert(featureData, defaultStorage, overwrite, writeOptions);
  }

  public void insert(Dataset<Row> featureData, Storage storage, boolean overwrite, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    featureGroupEngine.saveDataframe(this, featureData, storage,
        overwrite ? SaveMode.Overwrite : SaveMode.Append, writeOptions);
  }

  public void delete() throws FeatureStoreException, IOException {
    featureGroupEngine.delete(this);
  }

  public void addTag(String name) throws FeatureStoreException, IOException {
    addTag(name, null);
  }

  public void addTag(String name, String value) throws FeatureStoreException, IOException {
    featureGroupEngine.addTag(this, name, value);
  }

  public Map<String, String> getTags() throws FeatureStoreException, IOException {
    return featureGroupEngine.getTags(this);
  }

  public String getTag(String name) throws FeatureStoreException, IOException {
    return featureGroupEngine.getTag(this, name);
  }

  public void deleteTag(String name) throws FeatureStoreException, IOException {
    featureGroupEngine.deleteTag(this, name);
  }
}
