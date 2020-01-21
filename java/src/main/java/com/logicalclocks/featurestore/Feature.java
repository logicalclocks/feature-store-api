package com.logicalclocks.featurestore;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class Feature {
  @Getter @Setter
  private String name;

  @Getter @Setter
  private String Type;

  @Getter @Setter
  private String onlineType;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private Boolean primary;

  @Getter @Setter
  private Boolean partition;

  public Feature(String name) {
    this.name = name;
  }
}
