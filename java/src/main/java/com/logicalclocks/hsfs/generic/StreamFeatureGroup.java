/*
 *  Copyright (c) 2022. Logical Clocks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.generic;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.logicalclocks.hsfs.generic.constructor.Query;
import com.logicalclocks.hsfs.generic.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.generic.metadata.FeatureGroupBase;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.avro.Schema;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class StreamFeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  private Boolean onlineEnabled;

  @Getter
  @Setter
  private StorageConnector onlineStorageConnector;

  @Getter
  @Setter
  private StorageConnector offlineStorageConnector;

  @Getter
  @Setter
  private String type = "streamFeatureGroupDTO";

  @Getter
  @Setter
  protected String location;

  @Getter
  @Setter
  private List<String> statisticColumns;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  @JsonIgnore
  // This is only used in the client. In the server they are aggregated in the `features` field
  private String hudiPrecombineKey;

  @JsonIgnore
  private String avroSchema;

  @Getter(onMethod = @__(@Override))
  @Setter
  private String onlineTopicName;

  @Setter
  private DeltaStreamerJobConf deltaStreamerJobConf;
  private FeatureGroupUtils utils = new FeatureGroupUtils();

  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    if (avroSchema == null) {
      avroSchema = utils.getAvroSchema(this);
    }
    return avroSchema;
  }

  @Override
  public Query selectFeatures(List<Feature> features) {
    return null;
  }

  @Override
  public Query selectAll() {
    return null;
  }

  @Override
  public Query selectExceptFeatures(List<Feature> features) {
    return null;
  }

  @Override
  public Query selectExcept(List<String> features) {
    return null;
  }

  @Override
  public Object read(boolean online, Map<String, String> readOptions) throws FeatureStoreException, IOException {
    return null;
  }

  @Override
  public void updateFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void updateFeatures(Feature feature) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void appendFeatures(List<Feature> features) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public void appendFeatures(Feature features) throws FeatureStoreException, IOException, ParseException {

  }

  @Override
  public <T> T computeStatistics() throws FeatureStoreException, IOException {
    return null;
  }

  @JsonIgnore
  public List<String> getComplexFeatures() {
    return utils.getComplexFeatures(features);
  }

  @JsonIgnore
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    return utils.getFeatureAvroSchema(featureName, utils.getDeserializedAvroSchema(getAvroSchema()));
  }

  @JsonIgnore
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getEncodedAvroSchema(getDeserializedAvroSchema(), utils.getComplexFeatures(features));
  }

  @JsonIgnore
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getDeserializedAvroSchema(getAvroSchema());
  }

  @Override
  public TimeTravelFormat getTimeTravelFormat() {
    return null;
  }
}
