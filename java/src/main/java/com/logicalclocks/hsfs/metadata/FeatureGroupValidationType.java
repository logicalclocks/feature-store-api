package com.logicalclocks.hsfs.metadata;

public enum FeatureGroupValidationType {
  NONE(0),  // Data validation not performed on feature group
  ON_SUCCESS(1),// Data validation is performed and feature group is updated if validation status is "Success"
  ON_WARNING(2),// Data validation is performed and feature group is updated if validation status is "Warning" or lower
  ON_FAILURE(3);// Data validation is performed and feature group is updated if validation status is "Failure" or lower

  private final int severity;

  private FeatureGroupValidationType(int severity) {
    this.severity = severity;
  }

  public int getSeverity() {
    return severity;
  }

  public static FeatureGroupValidationType fromSeverity(int v) {
    for (FeatureGroupValidationType c : FeatureGroupValidationType.values()) {
      if (c.severity == v) {
        return c;
      }
    }
    throw new IllegalArgumentException("" + v);
  }
}
