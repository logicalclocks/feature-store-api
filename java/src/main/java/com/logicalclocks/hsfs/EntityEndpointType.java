package com.logicalclocks.hsfs;

public enum EntityEndpointType {
  FEATURE_GROUP("featuregroups"),
  TRAINING_DATASET("trainingdatasets");

  private final String value;

  private EntityEndpointType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
