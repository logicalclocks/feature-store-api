package com.logicalclocks.hsfs;

import com.logicalclocks.hsfs.metadata.Query;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;

public class OnDemandFeatureGroup implements FeatureGroup {

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

  @Builder
  // TODO(Fabio): here to be consistent we should also allow people to pass strings instead of feature objects
  public OnDemandFeatureGroup(FeatureStore featureStore, String name, Integer version, String description)
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

  @Override
  public Query selectFeatures(List<Feature> features) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Query selectAll() throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public Dataset<Row> read() {
    return null;
  }

  @Override
  public void show(int n) {

  }
}
