package com.logicalclocks.hsfs.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@ToString
public class KafkaClusterInfo extends RestDto<KafkaClusterInfo> {

  @Getter
  @Setter
  private List<String> brokers;
}
