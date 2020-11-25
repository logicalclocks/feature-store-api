package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
  private List<ExpectationResult> results;
  @Getter @Setter
  private String validationPath;
  @Getter @Setter
  private ExpectationResult.Status status;

  public String getCommitTimeAsDateTimeFormat() {
    Instant instant = Instant.ofEpochSecond(commitTime);
    return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
  }



  @Override
  public String toString() {
    return "FeatureGroupValidation{"
      + "validationTime=" + validationTime
      + ", results='" + results + '\''
      + ", path='" + validationPath + '\''
      + ", status='" + status + '\''
      + '}';
  }
}
