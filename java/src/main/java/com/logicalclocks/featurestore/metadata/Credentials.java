package com.logicalclocks.featurestore.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class Credentials {

  @Getter @Setter
  private String kStore;
  @Getter @Setter
  private String tStore;
  @Getter @Setter
  private String password;
}
