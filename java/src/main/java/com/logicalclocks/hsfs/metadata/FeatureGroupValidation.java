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
public class FeatureGroupValidation extends RestDto<FeatureGroupValidation> {

  @Getter @Setter
  private String validationTime;
  @Getter @Setter
  private Long commitTime;
  @Getter @Setter
  private List<DataValidationResult> validations;
  @Getter @Setter
  private String validationPath;
  @Getter @Setter
  private DataValidationResult.Status status;


  @Override
  public String toString() {
    return "DataValidationResults{"
      + "validationTime=" + validationTime
      + ", results='" + validations + '\''
      + ", path='" + validationPath + '\''
      + ", status='" + status + '\''
      + '}';
  }
}
