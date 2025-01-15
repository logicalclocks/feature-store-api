/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.util.Constants;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.CollectionUtils;

import java.io.IOException;
import java.time.Instant;
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
    @JsonSubTypes.Type(value = StorageConnector.HopsFsConnector.class, name = "HOPSFS"),
    @JsonSubTypes.Type(value = StorageConnector.S3Connector.class, name = "S3"),
    @JsonSubTypes.Type(value = StorageConnector.RedshiftConnector.class, name = "REDSHIFT"),
    @JsonSubTypes.Type(value = StorageConnector.AdlsConnector.class, name = "ADLS"),
    @JsonSubTypes.Type(value = StorageConnector.SnowflakeConnector.class, name = "SNOWFLAKE"),
    @JsonSubTypes.Type(value = StorageConnector.JdbcConnector.class, name = "JDBC"),
    @JsonSubTypes.Type(value = StorageConnector.KafkaConnector.class, name = "KAFKA"),
    @JsonSubTypes.Type(value = StorageConnector.GcsConnector.class, name = "GCS"),
    @JsonSubTypes.Type(value = StorageConnector.BigqueryConnector.class, name = "BIGQUERY")
})
public abstract class StorageConnector {

  @Getter @Setter
  protected StorageConnectorType storageConnectorType;

  @Getter @Setter
  protected Integer id;

  @Getter @Setter
  protected String name;

  @Getter @Setter
  protected String description;

  @Getter @Setter
  protected Integer featurestoreId;

  protected StorageConnectorApi storageConnectorApi = new StorageConnectorApi();

  protected static final Logger LOGGER = LoggerFactory.getLogger(StorageConnector.class);

  public StorageConnector refetch() throws FeatureStoreException, IOException {
    return  storageConnectorApi.get(getFeaturestoreId(), getName(), StorageConnector.class);
  }

  @JsonIgnore
  public abstract String getPath(String subPath) throws FeatureStoreException;

  public abstract Map<String, String> sparkOptions() throws IOException, FeatureStoreException;

  public static class HopsFsConnector extends StorageConnector {

    @Getter @Setter
    protected String hopsfsPath;

    @Getter @Setter
    protected String datasetName;

    public Map<String, String> sparkOptions() {
      return new HashMap<>();
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return hopsfsPath + "/" + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }
  }

  public static class S3Connector extends StorageConnector {

    @Getter @Setter
    protected String accessKey;

    @Getter @Setter
    protected String secretKey;

    @Getter @Setter
    protected String serverEncryptionAlgorithm;

    @Getter @Setter
    protected String serverEncryptionKey;

    @Getter @Setter
    protected String bucket;

    @Getter @Setter
    protected String path;

    @Getter @Setter
    protected String region;

    @Getter @Setter
    protected String sessionToken;

    @Getter @Setter
    protected String iamRole;

    @Getter @Setter
    protected List<Option> arguments;

    @JsonIgnore
    public String getPath(String subPath) {
      return "s3://" + bucket
          + (Strings.isNullOrEmpty(path) ? "" : "/" + path) + "/"
          + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }

    @Override
    public Map<String, String> sparkOptions() {
      if (!CollectionUtils.isNullOrEmpty(arguments)) {
        return arguments.stream().collect(Collectors.toMap(Option::getName, Option::getValue));
      }
      return new HashMap<>();
    }

    public void update() throws FeatureStoreException, IOException {
      S3Connector updatedConnector = (S3Connector) refetch();
      this.accessKey = updatedConnector.getAccessKey();
      this.secretKey = updatedConnector.getSecretKey();
      this.sessionToken = updatedConnector.getSessionToken();
    }
  }

  public static class RedshiftConnector extends StorageConnector {

    @Getter @Setter
    protected String clusterIdentifier;

    @Getter @Setter
    protected String databaseDriver;

    @Getter @Setter
    protected String databaseEndpoint;

    @Getter @Setter
    protected String databaseName;

    @Getter @Setter
    protected Integer databasePort;

    @Getter @Setter
    protected String tableName;

    @Getter @Setter
    protected String databaseUserName;

    @Getter @Setter
    protected Boolean autoCreate;

    @Getter @Setter
    protected String databasePassword;

    @Getter @Setter
    protected String databaseGroup;

    @Getter @Setter
    protected String iamRole;

    @Getter @Setter
    protected List<Option> arguments;

    @Getter @Setter
    protected Instant expiration;

    @Override
    public Map<String, String> sparkOptions() {
      String constr =
          "jdbc:redshift://" + clusterIdentifier + "." + databaseEndpoint + ":" + databasePort + "/" + databaseName;
      if (arguments != null && !arguments.isEmpty()) {
        constr += "?" + arguments.stream()
            .map(arg -> arg.getName() + (arg.getValue() != null ? "=" + arg.getValue() : ""))
            .collect(Collectors.joining(","));
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
    public String getPath(String subPath) {
      return null;
    }

    public void update() throws FeatureStoreException, IOException {
      RedshiftConnector updatedConnector = (RedshiftConnector) refetch();
      this.databaseUserName = updatedConnector.getDatabaseUserName();
      this.expiration = updatedConnector.getExpiration();
      this.databasePassword = updatedConnector.getDatabasePassword();
    }
  }

  public static class AdlsConnector extends StorageConnector {

    @Getter @Setter
    protected Integer generation;

    @Getter @Setter
    protected String directoryId;

    @Getter @Setter
    protected String applicationId;

    @Getter @Setter
    protected String serviceCredential;

    @Getter @Setter
    protected String accountName;

    @Getter @Setter
    protected String containerName;

    @Getter @Setter
    protected List<Option> sparkOptions;

    @JsonIgnore
    public String getPath(String subPath) {
      return (this.generation == 2
          ? "abfss://" + this.containerName + "@" + this.accountName + ".dfs.core.windows.net/"
          : "adl://" + this.accountName + ".azuredatalakestore.net/")
          + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }

    @Override
    public Map<String, String> sparkOptions() {
      Map<String, String> options = new HashMap<>();
      sparkOptions.stream().forEach(option -> options.put(option.getName(), option.getValue()));
      return options;
    }
  }

  public static class SnowflakeConnector extends StorageConnector {

    @Getter @Setter
    protected String url;

    @Getter @Setter
    protected String user;

    @Getter @Setter
    protected String password;

    @Getter @Setter
    protected String token;

    @Getter @Setter
    protected String database;

    @Getter @Setter
    protected String schema;

    @Getter @Setter
    protected String warehouse;

    @Getter @Setter
    protected String role;

    @Getter @Setter
    protected String table;

    @Getter @Setter
    protected String application;

    @Getter @Setter
    protected List<Option> sfOptions;

    public String account() {
      return this.url.replace("https://", "").replace(".snowflakecomputing.com", "");
    }

    @Override
    public Map<String, String> sparkOptions() {
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
      if (!Strings.isNullOrEmpty(application)) {
        options.put(Constants.SNOWFLAKE_APPLICATION, application);
      }
      if (sfOptions != null && !sfOptions.isEmpty()) {
        Map<String, String> argOptions = sfOptions.stream()
            .collect(Collectors.toMap(Option::getName, Option::getValue));
        options.putAll(argOptions);
      }
      return options;
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }

  public static class JdbcConnector extends StorageConnector {

    @Getter @Setter
    protected String connectionString;

    @Getter @Setter
    protected List<Option> arguments;

    @Override
    public Map<String, String> sparkOptions() {
      Map<String, String> options = new HashMap<>();
      if (arguments != null && !arguments.isEmpty()) {
        Map<String, String> readOptions = arguments.stream()
            .collect(Collectors.toMap(Option::getName, Option::getValue));
        options.putAll(readOptions);
      }
      options.put(Constants.JDBC_URL, connectionString);
      return options;
    }

    public void update() throws FeatureStoreException, IOException {
      JdbcConnector updatedConnector = (JdbcConnector) refetch();
      this.connectionString = updatedConnector.getConnectionString();
      this.arguments = updatedConnector.getArguments();
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }

  public static class KafkaConnector extends StorageConnector {

    public static final String sparkFormat = "kafka";

    @Getter @Setter
    protected String bootstrapServers;

    @Getter @Setter
    protected SecurityProtocol securityProtocol;

    @Getter @Setter
    protected String sslTruststoreLocation;

    @Getter @Setter
    protected String sslTruststorePassword;

    @Getter @Setter
    protected String sslKeystoreLocation;

    @Getter @Setter
    protected String sslKeystorePassword;

    @Getter @Setter
    protected String sslKeyPassword;

    @Getter @Setter
    protected SslEndpointIdentificationAlgorithm sslEndpointIdentificationAlgorithm;

    @Getter @Setter
    protected List<Option> options;

    @Getter @Setter
    protected Boolean externalKafka;

    public Map<String, String> kafkaOptions() throws FeatureStoreException {
      HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();
      Map<String, String> config = new HashMap<>();

      // set kafka storage connector options
      if (this.options != null && !this.options.isEmpty()) {
        Map<String, String> argOptions = this.options.stream()
                .collect(Collectors.toMap(Option::getName, Option::getValue));
        config.putAll(argOptions);
      }

      // set connection properties
      config.put(Constants.KAFKA_BOOTSTRAP_SERVERS, bootstrapServers);
      config.put(Constants.KAFKA_SECURITY_PROTOCOL, securityProtocol.toString());

      // set ssl
      config.put(Constants.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, sslEndpointIdentificationAlgorithm.getValue());

      if (!externalKafka) {
        sslTruststoreLocation = client.getTrustStorePath();
        sslTruststorePassword = client.getCertKey();
        sslKeystoreLocation = client.getKeyStorePath();
        sslKeystorePassword = client.getCertKey();
        sslKeyPassword = client.getCertKey();
      }

      if (sslTruststoreLocation != null) {
        config.put(Constants.KAFKA_SSL_TRUSTSTORE_LOCATION, sslTruststoreLocation);
      }
      if (sslTruststorePassword != null) {
        config.put(Constants.KAFKA_SSL_TRUSTSTORE_PASSWORD, sslTruststorePassword);
      }
      if (sslKeystoreLocation != null) {
        config.put(Constants.KAFKA_SSL_KEYSTORE_LOCATION, sslKeystoreLocation);
      }
      if (sslKeystorePassword != null) {
        config.put(Constants.KAFKA_SSL_KEYSTORE_PASSWORD, sslKeystorePassword);
      }
      if (sslKeyPassword != null) {
        config.put(Constants.KAFKA_SSL_KEY_PASSWORD, sslKeyPassword);
      }

      if (externalKafka) {
        LOGGER.info("Getting connection details to externally managed Kafka cluster. "
            + "Make sure that the topic being used exists.");
      }

      return config;
    }

    @Override
    public Map<String, String> sparkOptions() throws FeatureStoreException {
      Map<String, String> config = new HashMap<>();
      for (Map.Entry<String, String> entry: kafkaOptions().entrySet()) {
        config.put(String.format("%s.%s", sparkFormat, entry.getKey()), entry.getValue());
      }
      return config;
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }

  public static class GcsConnector extends StorageConnector {
    @Getter  @Setter
    protected String keyPath;
    @Getter @Setter
    protected String algorithm;
    @Getter @Setter
    protected String encryptionKey;
    @Getter @Setter
    protected String encryptionKeyHash;
    @Getter @Setter
    protected String bucket;

    public GcsConnector() {
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return "gs://" + bucket + "/"  + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }

    @Override
    public Map<String, String> sparkOptions() {
      return new HashMap<>();
    }

  }

  public static class BigqueryConnector extends StorageConnector {

    @Getter @Setter
    protected String keyPath;

    @Getter @Setter
    protected String parentProject;

    @Getter @Setter
    protected String queryProject;

    @Getter @Setter
    protected String dataset;

    @Getter @Setter
    protected String queryTable;

    @Getter @Setter
    protected String materializationDataset;

    @Getter @Setter
    protected List<Option>  arguments;

    /**
     * Set spark options specific to BigQuery.
     * @return Map
     */
    @Override
    public Map<String, String> sparkOptions() {
      Map<String, String> options = new HashMap<>();

      options.put(Constants.BIGQ_PARENT_PROJECT, parentProject);
      if (!Strings.isNullOrEmpty(materializationDataset)) {
        options.put(Constants.BIGQ_MATERIAL_DATASET, materializationDataset);
        options.put(Constants.BIGQ_VIEWS_ENABLED,"true");
      }
      if (!Strings.isNullOrEmpty(queryProject)) {
        options.put(Constants.BIGQ_PROJECT, queryProject);
      }
      if (!Strings.isNullOrEmpty(dataset)) {
        options.put(Constants.BIGQ_DATASET, dataset);
      }
      if (arguments != null && !arguments.isEmpty()) {
        Map<String, String> argOptions = arguments.stream()
            .collect(Collectors.toMap(Option::getName, Option::getValue));
        options.putAll(argOptions);
      }

      return options;
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }
}
