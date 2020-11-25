package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FeatureGroupValidations {

  @Getter @Setter
  private String validationTime;
  @Getter @Setter
  private List<ExpectationResult> validations;

  @Override
  public String toString() {
    return "DataValidationResults{"
      + "validationTime=" + validationTime
      + ", results='" + validations + '\''
      + '}';
  }
}
