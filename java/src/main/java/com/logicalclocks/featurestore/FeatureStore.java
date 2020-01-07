package com.logicalclocks.featurestore;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class FeatureStore {

  @Getter
  private Integer id;

  @Getter
  private String name;

  /**
   * Get a feature group from the feature store
   * @param name: the name of the feature group
   * @param version: the version of the feature group
   * @return
   * @throws FeatureStoreException
   */
  public FeatureGroup getFeatureGroup(String name, Integer version) throws FeatureStoreException {
    if (Strings.isNullOrEmpty(name) || version == null) {
      throw new FeatureStoreException("Both name and version are required");
    }
    return null;
  }
}
