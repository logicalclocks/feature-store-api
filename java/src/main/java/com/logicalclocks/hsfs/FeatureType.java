package com.logicalclocks.hsfs;


import lombok.Getter;

public enum FeatureType {

  BIGINT("bigint"),
  DATE("date"),
  TIMESTAMP("timestamp");

  @Getter
  private String type;

  FeatureType(String type) {
    this.type = type;
  }

}
