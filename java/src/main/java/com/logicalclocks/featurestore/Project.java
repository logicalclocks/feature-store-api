package com.logicalclocks.featurestore;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class Project {

  @Getter @Setter
  private Integer projectId;
  @Getter @Setter
  private String projectName;
  @Getter @Setter
  private String owner;

}
