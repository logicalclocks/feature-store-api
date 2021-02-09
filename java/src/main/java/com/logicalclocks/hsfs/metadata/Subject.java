package com.logicalclocks.hsfs.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Subject {

  @Getter
  @Setter
  private Integer id;

  @Getter
  @Setter
  private String schema;

  @Getter
  @Setter
  private String subject;

  @Getter
  @Setter
  private Integer version;
}
