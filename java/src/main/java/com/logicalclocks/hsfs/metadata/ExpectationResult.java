package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import scala.Enumeration;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExpectationResult {

  @Getter @Setter
  private Status status;
  @Getter @Setter
  private String message;
  @Getter @Setter
  private String value;
  @Getter @Setter
  private Expectation expectation;

  public enum Status {
    NONE("None",0),
    SUCCESS("Success",1),
    WARNING("Warning",2),
    FAILURE("Failure",3);

    private final String name;
    private final int severity;

    Status(String name, int severity) {
      this.name = name;
      this.severity = severity;
    }

    public int getSeverity() {
      return severity;
    }

    public static Status fromString(String name) {
      return valueOf(name.toUpperCase());
    }

    public static Status fromDeequStatus(Enumeration.Value status) {
      if (status == com.amazon.deequ.constraints.ConstraintStatus.Failure()) {
        return FAILURE;
      } else if (status == com.amazon.deequ.constraints.ConstraintStatus.Success()) {
        return SUCCESS;
      } else {
        return NONE;
      }
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  @Override
  public String toString() {
    return "DataValidationResult{"
      + "status=" + status
      + ", message='" + message + '\''
      + ", value='" + value + '\''
      + ", expectation=" + expectation
      + '}';
  }
}
