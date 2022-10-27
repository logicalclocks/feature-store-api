/*
 *  Copyright (c) 2020-2022. Hopsworks AB
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

package com.logicalclocks.base;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import com.logicalclocks.base.metadata.Option;
import com.logicalclocks.base.metadata.StorageConnectorApi;
import com.logicalclocks.base.util.Constants;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "storageConnectorType", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = StorageConnectorBase.HopsFsConnectorBase.class, name = "HOPSFS"),
    @JsonSubTypes.Type(value = StorageConnectorBase.JdbcConnectorBase.class, name = "JDBC")
})
public abstract class StorageConnectorBase {

  @Getter @Setter
  protected StorageConnectorType storageConnectorType;

  @Getter @Setter
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private Integer featurestoreId;

  protected StorageConnectorApi storageConnectorApi = new StorageConnectorApi();

  public abstract StorageConnectorBase refetch() throws FeatureStoreException, IOException, FeatureStoreException;

  @JsonIgnore
  public abstract String getPath(String subPath) throws FeatureStoreException;

  public abstract Map<String, String> sparkOptions() throws IOException;

  public abstract Object read(String query, String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException;

  public static class HopsFsConnectorBase extends StorageConnectorBase {

    @Getter @Setter
    private String hopsfsPath;

    @Getter @Setter
    private String datasetName;

    public Map<String, String> sparkOptions() {
      return new HashMap<>();
    }

    @Override
    public Object read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {
      return null;
    }

    @Override
    public StorageConnectorBase refetch() throws FeatureStoreException, IOException, FeatureStoreException {
      return null;
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return hopsfsPath + "/" + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }
  }

  public static class JdbcConnectorBase extends StorageConnectorBase {

    @Getter @Setter
    private String connectionString;

    @Getter @Setter
    private List<Option> arguments;

    public Map<String, String> sparkOptions() {
      Map<String, String> readOptions = arguments.stream()
          .collect(Collectors.toMap(arg -> arg.getName(), arg -> arg.getValue()));
      readOptions.put(Constants.JDBC_URL, connectionString);
      return readOptions;
    }

    // this will be overridden in engines
    @Override
    public Object read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {
      return null;
    }

    // this will be overridden in engines
    public void update() throws FeatureStoreException, IOException {
    }

    @Override
    // this will be overridden in engines
    public StorageConnectorBase refetch() throws FeatureStoreException, IOException, FeatureStoreException {
      return null;
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }
}
