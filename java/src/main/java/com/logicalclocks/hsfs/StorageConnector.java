/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.util.Constants;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.parquet.Strings;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@NoArgsConstructor
@ToString
public class StorageConnector {

  @Getter
  @Setter
  private Integer id;

  @Getter
  @Setter
  private String name;

  @Getter
  @Setter
  private String accessKey;

  @Getter
  @Setter
  private String secretKey;

  @Getter
  @Setter
  private String serverEncryptionAlgorithm;

  @Getter
  @Setter
  private String serverEncryptionKey;

  @Getter
  @Setter
  private String bucket;

  @Getter
  @Setter
  private String clusterIdentifier;

  @Getter
  @Setter
  private String databaseDriver;

  @Getter
  @Setter
  private String databaseEndpoint;

  @Getter
  @Setter
  private String databaseName;

  @Getter
  @Setter
  private Integer databasePort;

  @Getter
  @Setter
  private String tableName;

  @Getter
  @Setter
  private String databaseUserName;

  @Getter
  @Setter
  private Boolean autoCreate;

  @Getter
  @Setter
  private String databaseGroup;

  @Getter
  @Setter
  private Date expiration;

  @Getter
  @Setter
  private String databasePassword;

  @Getter
  @Setter
  private String sessionToken;

  @Getter
  @Setter
  private String connectionString;

  @Getter
  @Setter
  private String arguments;

  @Getter
  @Setter
  private Integer generation;

  @Getter
  @Setter
  private String directoryId;

  @Getter
  @Setter
  private String applicationId;

  @Getter
  @Setter
  private String serviceCredentials;

  @Getter
  @Setter
  private String accountName;

  @Getter
  @Setter
  private String containerName;

  @Getter
  @Setter
  private String url;

  @Getter
  @Setter
  private String user;

  @Getter
  @Setter
  private String password;

  @Getter
  @Setter
  private String token;

  @Getter
  @Setter
  private String database;

  @Getter
  @Setter
  private String schema;

  @Getter
  @Setter
  private String warehouse;

  @Getter
  @Setter
  private String role;

  @Getter
  @Setter
  private String table;

  @Getter
  @Setter
  private List<Option> sparkOptions;

  @Getter
  @Setter
  private List<Option> sfOptions;

  @Getter
  @Setter
  private StorageConnectorType storageConnectorType;

  public String account() {
    return this.url.replace("https://", "").replace(".snowflakecomputing.com", "");
  }

  @JsonIgnore
  public Map<String, String> getSparkOptionsInt() throws FeatureStoreException {
    switch (storageConnectorType) {
      case JDBC:
        return getJdbcOptions();
      case REDSHIFT:
        return getRedshiftOptions();
      case SNOWFLAKE:
        return getSnowflakeOptions();
      default:
        throw new FeatureStoreException("Spark options are not supported for connector " + storageConnectorType);
    }
  }

  @JsonIgnore
  private Map<String, String> getJdbcOptions() {
    Map<String, String> options = Arrays.stream(arguments.split(","))
        .map(arg -> arg.split("="))
        .collect(Collectors.toMap(a -> a[0], a -> a[1]));
    options.put(Constants.JDBC_URL, connectionString);
    return options;
  }

  @JsonIgnore
  private Map<String, String> getRedshiftOptions() {
    String constr =
        "jdbc:redshift://" + clusterIdentifier + "." + databaseEndpoint + ":" + databasePort + "/" + databaseName;
    if (!Strings.isNullOrEmpty(arguments)) {
      constr += "?" + arguments;
    }
    Map<String, String> options = new HashMap<>();
    options.put(Constants.JDBC_DRIVER, databaseDriver);
    options.put(Constants.JDBC_URL, constr);
    options.put(Constants.JDBC_USER, databaseUserName);
    options.put(Constants.JDBC_PWD, databasePassword);
    if (!Strings.isNullOrEmpty(tableName)) {
      options.put(Constants.JDBC_TABLE, tableName);
    }
    return options;
  }

  @JsonIgnore
  public String getPath(String subPath) throws FeatureStoreException {
    switch (storageConnectorType) {
      case S3:
        return "s3://" + bucket + "/"  + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
      default:
        throw new FeatureStoreException(
            "Path method not supported for storage connector type: " + storageConnectorType);
    }
  }

  @JsonIgnore
  private Map<String, String> getSnowflakeOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(Constants.SNOWFLAKE_URL, url);
    options.put(Constants.SNOWFLAKE_SCHEMA, schema);
    options.put(Constants.SNOWFLAKE_DB, database);
    options.put(Constants.SNOWFLAKE_USER, user);
    if (!Strings.isNullOrEmpty(password)) {
      options.put(Constants.SNOWFLAKE_PWD, password);
    } else {
      options.put(Constants.SNOWFLAKE_AUTH, "oauth");
      options.put(Constants.SNOWFLAKE_TOKEN, token);
    }
    if (!Strings.isNullOrEmpty(warehouse)) {
      options.put(Constants.SNOWFLAKE_WAREHOUSE, warehouse);
    }
    if (!Strings.isNullOrEmpty(role)) {
      options.put(Constants.SNOWFLAKE_ROLE, role);
    }
    if (!Strings.isNullOrEmpty(table)) {
      options.put(Constants.SNOWFLAKE_TABLE, table);
    }
    if (sfOptions != null && !sfOptions.isEmpty()) {
      Map<String, String> argOptions = sfOptions.stream()
          .collect(Collectors.toMap(Option::getName, Option::getValue));
      options.putAll(argOptions);
    }
    return options;
  }
}
