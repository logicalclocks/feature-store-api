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
  private StorageConnectorType storageConnectorType;

  public Map<String, String> getSparkOptions() throws FeatureStoreException {
    if (StorageConnectorType.JDBC.equals(storageConnectorType)) {
      return getJdbcOptions();
    } else if (StorageConnectorType.REDSHIFT.equals(storageConnectorType)) {
      return getRedshiftOptions();
    }
    throw new FeatureStoreException("Spark options are not supported for connector " + storageConnectorType);
  }

  private Map<String, String> getJdbcOptions() throws FeatureStoreException {
    Map<String, String> options = Arrays.stream(arguments.split(","))
        .map(arg -> arg.split("="))
        .collect(Collectors.toMap(a -> a[0], a -> a[1]));
    options.put(Constants.JDBC_URL, connectionString);
    return options;
  }

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
}
