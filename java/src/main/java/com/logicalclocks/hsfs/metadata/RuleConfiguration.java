package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Arrays;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RuleConfiguration {

  @Getter @Setter
  private Rule.ConstraintGroupLevel level;

  @Getter @Setter
  private Double min;

  @Getter @Setter
  private Double max;

  @Getter @Setter
  private String pattern;

  @Getter @Setter
  private String[] legalValues;

  @Override
  public String toString() {
    return "RuleConfiguration{"
      + "level=" + level
      + ", min=" + min
      + ", max=" + max
      + ", pattern='" + pattern + '\''
      + ", legalValues=" + Arrays.toString(legalValues)
      + '}';
  }
}
