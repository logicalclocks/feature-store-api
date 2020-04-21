package com.logicalclocks.featurestore;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum DataFormat {
  @JsonProperty("csv")
  CSV,
  @JsonProperty("tsv")
  TSV,
  @JsonProperty("parquet")
  PARQUET,
  @JsonProperty("avro")
  AVRO,
  @JsonProperty("image")
  IMAGE,
  @JsonProperty("orc")
  ORC,
  @JsonProperty("tfrecords")
  TFRECORDS
}
