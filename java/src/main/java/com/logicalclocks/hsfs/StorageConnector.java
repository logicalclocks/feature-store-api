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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.logicalclocks.hsfs.engine.SparkEngine;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.util.Constants;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
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
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private Integer featurestoreId;

  protected StorageConnectorApi storageConnectorApi = new StorageConnectorApi();

  /**
   * Reads a query or a path into a dataframe using the storage connector.
   *
   * @param query By default, the storage connector will read the table configured together  with the connector, if any.
   *              It's possible to overwrite this by passing a SQL query here.
   * @param dataFormat When reading from object stores such as S3, HopsFS and ADLS, specify the file format to be read,
   *                  e.g. `csv`, `parquet`.
   * @param options Any additional key/value options to be passed to the connector.
   * @param path Path to be read from within the bucket of the storage connector. Not relevant for JDBC or database
   *             based connectors such as Snowflake, JDBC or Redshift.
   * @return DataFrame.
   * @throws FeatureStoreException If ...
   * @throws IOException If ...
   */
  public Object read(String query, String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
    return SparkEngine.getInstance().read(this, dataFormat, options, path);
  }

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

    @JsonIgnore
    public String getPath(String subPath) {
      return hopsfsPath + "/" + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }
  }

  public static class S3Connector extends StorageConnector {

    @Getter @Setter
    private String accessKey;

    @Getter @Setter
    private String secretKey;

    @Getter @Setter
    private String serverEncryptionAlgorithm;

    @Getter @Setter
    private String serverEncryptionKey;

    @Getter @Setter
    private String bucket;

    @Getter @Setter
    private String sessionToken;

    @Getter @Setter
    private String iamRole;

    @JsonIgnore
    public String getPath(String subPath) {
      return "s3://" + bucket + "/"  + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }

    public Map<String, String> sparkOptions() {
      return new HashMap<>();
    }

    public Object read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {
      update();
      return SparkEngine.getInstance().read(this, dataFormat, options, path);
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
    private String clusterIdentifier;

    @Getter @Setter
    private String databaseDriver;

    @Getter @Setter
    private String databaseEndpoint;

    @Getter @Setter
    private String databaseName;

    @Getter @Setter
    private Integer databasePort;

    @Getter @Setter
    private String tableName;

    @Getter @Setter
    private String databaseUserName;

    @Getter @Setter
    private Boolean autoCreate;

    @Getter @Setter
    private String databasePassword;

    @Getter @Setter
    private String databaseGroup;

    @Getter @Setter
    private String iamRole;

    @Getter @Setter
    private List<Option> arguments;

    @Getter @Setter
    private Instant expiration;

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

    public Object read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {
      update();
      Map<String, String> readOptions = sparkOptions();
      if (!Strings.isNullOrEmpty(query)) {
        readOptions.put("query", query);
      }
      return SparkEngine.getInstance().read(this, Constants.JDBC_FORMAT, readOptions, null);
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
    private Integer generation;

    @Getter @Setter
    private String directoryId;

    @Getter @Setter
    private String applicationId;

    @Getter @Setter
    private String serviceCredential;

    @Getter @Setter
    private String accountName;

    @Getter @Setter
    private String containerName;

    @Getter @Setter
    private List<Option> sparkOptions;

    @JsonIgnore
    public String getPath(String subPath) {
      return (this.generation == 2
          ? "abfss://" + this.containerName + "@" + this.accountName + ".dfs.core.windows.net/"
          : "adl://" + this.accountName + ".azuredatalakestore.net/")
          + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }

    public Map<String, String> sparkOptions() {
      Map<String, String> options = new HashMap<>();
      sparkOptions.stream().forEach(option -> options.put(option.getName(), option.getValue()));
      return options;
    }
  }

  public static class SnowflakeConnector extends StorageConnector {

    @Getter @Setter
    private String url;

    @Getter @Setter
    private String user;

    @Getter @Setter
    private String password;

    @Getter @Setter
    private String token;

    @Getter @Setter
    private String database;

    @Getter @Setter
    private String schema;

    @Getter @Setter
    private String warehouse;

    @Getter @Setter
    private String role;

    @Getter @Setter
    private String table;

    @Getter @Setter
    private String application;

    @Getter @Setter
    private List<Option> sfOptions;

    public String account() {
      return this.url.replace("https://", "").replace(".snowflakecomputing.com", "");
    }

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

    public Object read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {
      Map<String, String> readOptions = sparkOptions();
      if (!Strings.isNullOrEmpty(query)) {
        // if table also specified we override to use query
        readOptions.remove(Constants.SNOWFLAKE_TABLE);
        readOptions.put("query", query);
      }
      return SparkEngine.getInstance().read(this, Constants.SNOWFLAKE_FORMAT, readOptions, null);
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }

  public static class JdbcConnector extends StorageConnector {

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

    @Override
    public Object read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {
      update();
      Map<String, String> readOptions = sparkOptions();
      if (!Strings.isNullOrEmpty(query)) {
        readOptions.put("query", query);
      }
      return SparkEngine.getInstance().read(this, Constants.JDBC_FORMAT, readOptions, null);
    }

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

  public static class KafkaConnector extends StorageConnector {

    public static final String sparkFormat = "kafka";

    @Getter @Setter
    private String bootstrapServers;

    @Getter @Setter
    private SecurityProtocol securityProtocol;

    @Getter
    private String sslTruststoreLocation;

    @Getter @Setter
    private String sslTruststorePassword;

    @Getter
    private String sslKeystoreLocation;

    @Getter @Setter
    private String sslKeystorePassword;

    @Getter @Setter
    private String sslKeyPassword;

    @Getter @Setter
    private SslEndpointIdentificationAlgorithm sslEndpointIdentificationAlgorithm;

    @Getter @Setter
    private List<Option> options;

    public void setSslTruststoreLocation(String sslTruststoreLocation) {
      this.sslTruststoreLocation = SparkEngine.getInstance().addFile(sslTruststoreLocation);
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
      this.sslKeystoreLocation = SparkEngine.getInstance().addFile(sslKeystoreLocation);
    }

    public Map<String, String> sparkOptions() {
      Map<String, String> options = new HashMap<>();
      options.put(Constants.KAFKA_BOOTSTRAP_SERVERS, bootstrapServers);
      options.put(Constants.KAFKA_SECURITY_PROTOCOL, securityProtocol.toString());
      if (!Strings.isNullOrEmpty(sslTruststoreLocation)) {
        options.put(Constants.KAFKA_SSL_TRUSTSTORE_LOCATION, sslTruststoreLocation);
      }
      if (!Strings.isNullOrEmpty(sslTruststorePassword)) {
        options.put(Constants.KAFKA_SSL_TRUSTSTORE_PASSWORD, sslTruststorePassword);
      }
      if (!Strings.isNullOrEmpty(sslKeystoreLocation)) {
        options.put(Constants.KAFKA_SSL_KEYSTORE_LOCATION, sslKeystoreLocation);
      }
      if (!Strings.isNullOrEmpty(sslKeystorePassword)) {
        options.put(Constants.KAFKA_SSL_KEYSTORE_PASSWORD, sslKeystorePassword);
      }
      if (!Strings.isNullOrEmpty(sslKeyPassword)) {
        options.put(Constants.KAFKA_SSL_KEY_PASSWORD, sslKeyPassword);
      }
      // can be empty string
      if (sslEndpointIdentificationAlgorithm != null) {
        options.put(
            Constants.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, sslEndpointIdentificationAlgorithm.getValue());
      }
      if (this.options != null && !this.options.isEmpty()) {
        Map<String, String> argOptions = this.options.stream()
            .collect(Collectors.toMap(Option::getName, Option::getValue));
        options.putAll(argOptions);
      }
      return options;
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }

    public Object read(String query, String dataFormat, Map<String, String> options, String path) {
      throw new NotSupportedException("Reading a Kafka Stream into a static Spark Dataframe is not supported.");
    }

    public Object readStream(String topic, boolean topicPattern, String messageFormat, String schema,
        Map<String, String> options, boolean includeMetadata) throws FeatureStoreException, IOException {
      if (!Arrays.asList("avro", "json", null).contains(messageFormat.toLowerCase())) {
        throw new IllegalArgumentException("Can only read JSON and AVRO encoded records from Kafka.");
      }

      if (topicPattern) {
        options.put("subscribePattern", topic);
      } else {
        options.put("subscribe", topic);
      }

      return SparkEngine.getInstance().readStream(this, sparkFormat, messageFormat.toLowerCase(),
          schema, options, includeMetadata);
    }
  }

  public static class GcsConnector extends StorageConnector {
    @Getter  @Setter
    private String keyPath;
    @Getter @Setter
    private String algorithm;
    @Getter @Setter
    private String encryptionKey;
    @Getter @Setter
    private String encryptionKeyHash;
    @Getter @Setter
    private String bucket;

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

    public void prepareSpark() throws FeatureStoreException, IOException {
      SparkEngine.getInstance().setupConnectorHadoopConf(this);
    }

  }

  public static class BigqueryConnector extends StorageConnector {

    @Getter @Setter
    private String keyPath;

    @Getter @Setter
    private String parentProject;

    @Getter @Setter
    private String queryProject;

    @Getter @Setter
    private String dataset;

    @Getter @Setter
    private String queryTable;

    @Getter @Setter
    private String materializationDataset;

    @Getter @Setter
    private List<Option>  arguments;

    /**
     * Set spark options specific to BigQuery.
     * @return Map
     * @throws IOException IOException
     */
    public Map<String, String> sparkOptions() throws IOException {
      Map<String, String> options = new HashMap<>();

      // Base64 encode the credentials file
      String localKeyPath = SparkEngine.getInstance().addFile(keyPath);
      byte[] fileContent = Files.readAllBytes(Paths.get(localKeyPath));
      options.put(Constants.BIGQ_CREDENTIALS, Base64.getEncoder().encodeToString(fileContent));

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

    /**
     * If Table options are set in the storage connector, set path to table.
     * Else use the query argument to set as path.
     * @param query query string
     * @param dataFormat dataFormat
     * @param options options
     * @param path path
     * @return Dataframe
     * @throws FeatureStoreException FeatureStoreException
     * @throws IOException IOException
     */
    @Override
    public Object read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {

      Map<String, String> readOptions = sparkOptions();
      // merge user spark options on top of default spark options
      if (options != null && !options.isEmpty()) {
        readOptions.putAll(options);
      }

      if (!Strings.isNullOrEmpty(query)) {
        path = query;
      } else if (!Strings.isNullOrEmpty(queryTable)) {
        path = queryTable;
      } else if (!Strings.isNullOrEmpty(path)) {
        path = path;
      } else {
        throw new IllegalArgumentException("Either query should be provided"
                                             + " or Query Project,Dataset and Table should be set");
      }

      return SparkEngine.getInstance().read(this, Constants.BIGQUERY_FORMAT, readOptions, path);
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }
}
