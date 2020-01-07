package com.logicalclocks.featurestore;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class FeatureGroup {

  @Getter
  private String name;

  @Getter
  private Integer version;

}
