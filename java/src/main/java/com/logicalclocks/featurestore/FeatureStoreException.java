package com.logicalclocks.featurestore;

public class FeatureStoreException extends Exception {

  public FeatureStoreException(String msg) {
    super(msg);
  }

  public FeatureStoreException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
