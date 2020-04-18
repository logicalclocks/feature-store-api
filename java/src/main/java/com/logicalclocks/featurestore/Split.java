package com.logicalclocks.featurestore;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class Split {
  @Getter @Setter
  private String name;

  @Getter @Setter
  private Float percentage;
}
