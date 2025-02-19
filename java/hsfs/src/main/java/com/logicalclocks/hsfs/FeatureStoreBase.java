/*
 *  Copyright (c) 2020-2023. Hopsworks AB
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.logicalclocks.hsfs.constructor.QueryBase;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.metadata.TrainingDatasetApi;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class FeatureStoreBase<T2 extends QueryBase> {

  @Getter
  @Setter
  @JsonProperty("featurestoreId")
  private Integer id;

  @Getter
  @Setter
  @JsonProperty("featurestoreName")
  private String name;

  @Getter
  @Setter
  private Integer projectId;

  protected FeatureGroupApi featureGroupApi;
  protected TrainingDatasetApi trainingDatasetApi;
  protected StorageConnectorApi storageConnectorApi;

  protected static final Logger LOGGER = LoggerFactory.getLogger(FeatureStoreBase.class);

  protected static final Integer DEFAULT_VERSION = 1;

  /**
   * Create a feature group object.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @param description descrption of the feature group
   * @param onlineEnabled whether the feature group should be online enabled
   * @param timeTravelFormat the data format to use to store the offline data
   * @param primaryKeys list of primary keys
   * @param partitionKeys list of partition keys
   * @param eventTime the feature/column to use as event time
   * @param hudiPrecombineKey if the timeTravelFormat is set to hudi, the feature/column to use as precombine key
   * @param features the list of feature objects if defined explicitly
   * @param statisticsConfig the statistics configuration for the feature group
   * @param storageConnector the storage connector to use to store the offline
   *                         feature data (Default stored internally in Hopsworks).
   * @param path the path on the storage where to store the feature data.
   *
   * @return The feature group metadata object.
   */
  public abstract FeatureGroupBase createStreamFeatureGroup(@NonNull String name,
                                                            Integer version,
                                                            String description,
                                                            Boolean onlineEnabled,
                                                            TimeTravelFormat timeTravelFormat,
                                                            List<String> primaryKeys,
                                                            List<String> partitionKeys,
                                                            String eventTime,
                                                            String hudiPrecombineKey,
                                                            List<Feature> features,
                                                            StatisticsConfig statisticsConfig,
                                                            StorageConnector storageConnector,
                                                            String path);

  /**
   * Get a feature group metadata object or create a new one if it doesn't exists.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StreamFeatureGroup streamFeatureGroup = featureStore.getOrCreateStreamFeatureGroup(
   *                 "doc_example",
   *                 1,
   *                 "Feature group for example in documentation",
   *                 true,
   *                 TimeTravelFormat.HUDI,
   *                 Collections.singletonList("primary_key"),
   *                 Collections.singletonList("partition_key"),
   *                 "event_time",
   *                 null,
   *                 features,
   *                 null,
   *                 new StatisticsConfig(true, true, true, true),
   *                 null
   *         );
   *
   *         streamFeatureGroup.save()
   * }
   * </pre>
   *
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @param description descrption of the feature group
   * @param onlineEnabled whether the feature group should be online enabled
   * @param timeTravelFormat the data format to use to store the offline data
   * @param primaryKeys list of primary keys
   * @param partitionKeys list of partition keys
   * @param eventTime the feature/column to use as event time
   * @param hudiPrecombineKey if the timeTravelFormat is set to hudi, the feature/column to use as precombine key
   * @param features the list of feature objects if defined explicitly
   * @param statisticsConfig the statistics configuration for the feature group
   * @param storageConnector the storage connector to use to store the offline
   *                         feature data (Default stored internally in Hopsworks).
   * @param path the path on the storage where to store the feature data.
   *
   * @return The feature group metadata object.
   */
  public abstract FeatureGroupBase getOrCreateStreamFeatureGroup(@NonNull String name,
                                                            Integer version,
                                                            String description,
                                                            Boolean onlineEnabled,
                                                            TimeTravelFormat timeTravelFormat,
                                                            List<String> primaryKeys,
                                                            List<String> partitionKeys,
                                                            String eventTime,
                                                            String hudiPrecombineKey,
                                                            List<Feature> features,
                                                            StatisticsConfig statisticsConfig,
                                                            StorageConnector storageConnector,
                                                            String path) throws IOException, FeatureStoreException;

  /**
   * Get a feature group object from the feature store.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract Object getStreamFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException;

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  public abstract Object getStreamFeatureGroup(String name) throws FeatureStoreException, IOException;

  public abstract Object getFeatureView(String name) throws FeatureStoreException, IOException;

  public abstract Object getFeatureView(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException;

  /**
   * Get a previously created storage connector from the feature store.
   *
   * <p>Storage connectors encapsulate all information needed for the execution engine to read and write to a specific
   * storage.
   *
   * <p>If you want to connect to the online feature store, see the getOnlineStorageConnector` method to get the
   * JDBC connector for the Online Feature Store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector sc = fs.getStorageConnector("sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector getStorageConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.class);
  }

  /**
   * Get a previously created HopsFs compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.HopsFsConnector hfsSc = fs.getHopsFsConnector("hfs_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.HopsFsConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.HopsFsConnector getHopsFsConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.HopsFsConnector.class);
  }

  /**
   * Get a previously created JDBC compliant storage connector from the feature store.
   *
   * <p>If you want to connect to the online feature store, see the getOnlineStorageConnector` method to get the
   * JDBC connector for the Online Feature Store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.JdbcConnector jdbcSc = fs.getJdbcConnector("jdbc_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the jdbc storage connector to retrieve.
   * @return StorageConnector.JdbcConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.JdbcConnector getJdbcConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.JdbcConnector.class);
  }

  /**
   * Get a previously created JDBC compliant storage connector from the feature store
   * to connect to the online feature store.
   *
   * <pre>
   * {@code
   *        //get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.JdbcConnector onlineSc = fs.getOnlineStorageConnector("online_sc_name");
   * }
   * </pre>
   *
   * @return StorageConnector.JdbcConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.JdbcConnector getOnlineStorageConnector() throws FeatureStoreException, IOException {
    return storageConnectorApi.getOnlineStorageConnector(this, StorageConnector.JdbcConnector.class);
  }

  /**
   * Get a previously created S3 compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.S3Connector s3Sc = fs.getS3Connector("s3_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.S3Connector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.S3Connector getS3Connector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.S3Connector.class);
  }

  /**
   * Get a previously created Redshift compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.RedshiftConnector rshSc = fs.getRedshiftConnector("rsh_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.RedshiftConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.RedshiftConnector getRedshiftConnector(String name)
      throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.RedshiftConnector.class);
  }

  /**
   * Get a previously created Snowflake compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.SnowflakeConnector snflSc = fs.getSnowflakeConnector("snfl_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.SnowflakeConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.SnowflakeConnector getSnowflakeConnector(String name)
      throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.SnowflakeConnector.class);
  }

  /**
   * Get a previously created Adls compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.AdlsConnectorr adlslSc = fs.getAdlsConnector("adls_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.AdlsConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.AdlsConnector getAdlsConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.AdlsConnector.class);
  }

  /**
   * Get a previously created Kafka compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.KafkaConnector kafkaSc = fs.getKafkaConnector("kafka_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.KafkaConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.KafkaConnector getKafkaConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.KafkaConnector.class);
  }

  /**
   * Get a previously created BigQuery compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.BigqueryConnector bigqSc = fs.getBigqueryConnector("bigq_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.BigqueryConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.BigqueryConnector getBigqueryConnector(String name) throws FeatureStoreException,
      IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.BigqueryConnector.class);
  }

  /**
   * Get a previously created Gcs compliant storage connector from the feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        StorageConnector.GcsConnector gcsSc = fs.getGcsConnector("gsc_sc_name");
   * }
   * </pre>
   *
   * @param name Name of the storage connector to retrieve.
   * @return StorageConnector.GcsConnector Storage connector object.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public StorageConnector.GcsConnector getGcsConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.GcsConnector.class);
  }

  @Override
  public String toString() {
    return "FeatureStore{"
        + "id=" + id
        + ", name='" + name + '\''
        + ", projectId=" + projectId
        + ", featureGroupApi=" + featureGroupApi
        + '}';
  }
}
