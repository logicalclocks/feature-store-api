/*
 *  Copyright (c) 2020-2022. Logical Clocks AB
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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.generic.metadata.Option;
import com.logicalclocks.hsfs.generic.metadata.StorageConnectorApi;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "storageConnectorType", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = StorageConnector.HopsFsConnector.class, name = "HOPSFS"),
})
public abstract class StorageConnector {

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

  public abstract Object read(String query, String dataFormat, Map<String, String> options, String path) throws FeatureStoreException, IOException;

  public StorageConnector refetch() throws FeatureStoreException, IOException {
    return  storageConnectorApi.get(getFeaturestoreId(), getName());
  }

  @JsonIgnore
  public abstract String getPath(String subPath) throws FeatureStoreException;

  public abstract Map<String, String> sparkOptions() throws IOException;

  public static class HopsFsConnector extends StorageConnector {

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

    @JsonIgnore
    public String getPath(String subPath) {
      return hopsfsPath + "/" + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }
  }

  public static abstract class JdbcConnector extends StorageConnector {

    @Getter @Setter
    private String connectionString;

    @Getter @Setter
    private List<Option> arguments;

    public abstract Map<String, String> sparkOptions();

    public abstract Object read(String query, String dataFormat, Map<String, String> options, String path) throws FeatureStoreException, IOException;

    public void update() throws FeatureStoreException, IOException {
      JdbcConnector updatedConnector = (JdbcConnector) refetch();
      this.connectionString = updatedConnector.connectionString;
      this.arguments = updatedConnector.arguments;
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }
}
