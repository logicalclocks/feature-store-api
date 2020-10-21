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
public class DataValidationResult {

  @Getter @Setter
  private Status status;
  @Getter @Setter
  private String message;
  @Getter @Setter
  private String value;
  @Getter @Setter
  private Rule rule;


  public enum Status {
    SUCCESS("Success"),
    FAILURE("Failure"),
    WARNING("Warning"),
    EMPTY("Empty");

    private final String name;

    Status(String name) {
      this.name = name;
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
        return EMPTY;
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
      + ", rule=" + rule
      + '}';
  }
}
