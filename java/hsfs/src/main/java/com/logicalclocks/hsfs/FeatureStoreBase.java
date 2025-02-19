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
   * Create an offline only Apache HUDI backed feature group object.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @param description descrption of the feature group
   * @param primaryKeys list of primary keys
   * @param partitionKeys list of partition keys
   * @param eventTime the feature/column to use as event time
   * @param features the list of feature objects if defined explicitly
   * @param statisticsConfig the statistics configuration for the feature group
   *
   * @return The feature group metadata object.
   */
  public FeatureGroupBase createStreamFeatureGroup(@NonNull String name,
                                                   Integer version,
                                                   String description,
                                                   List<String> primaryKeys,
                                                   List<String> partitionKeys,
                                                   String eventTime,
                                                   List<Feature> features,
                                                   StatisticsConfig statisticsConfig) {
    return createStreamFeatureGroup(name,
        version,
        description,
        false,
        TimeTravelFormat.HUDI,
        primaryKeys,
        partitionKeys,
        eventTime,
        null,
        features,
        statisticsConfig,
        null,
        null);
  }

  /**
   * Create an offline only feature group object.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @param description descrption of the feature group
   * @param timeTravelFormat the data format to use to store the offline data
   * @param primaryKeys list of primary keys
   * @param partitionKeys list of partition keys
   * @param eventTime the feature/column to use as event time
   * @param features the list of feature objects if defined explicitly
   * @param statisticsConfig the statistics configuration for the feature group
   *
   * @return The feature group metadata object.
   */
  public FeatureGroupBase createStreamFeatureGroup(@NonNull String name,
                                                   Integer version,
                                                   String description,
                                                   TimeTravelFormat timeTravelFormat,
                                                   List<String> primaryKeys,
                                                   List<String> partitionKeys,
                                                   String eventTime,
                                                   List<Feature> features,
                                                   StatisticsConfig statisticsConfig) {
    return createStreamFeatureGroup(name,
        version,
        description,
        false,
        timeTravelFormat,
        primaryKeys,
        partitionKeys,
        eventTime,
        null,
        features,
        statisticsConfig,
        null,
        null);
  }

  /**
   * Create a feature group object stored internally in Hopsworks.
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @param description descrption of the feature group
   * @param onlineEnabled whether the feature group should be online enabled
   * @param timeTravelFormat the data format to use to store the offline data
   * @param primaryKeys list of primary keys
   * @param partitionKeys list of partition keys
   * @param eventTime the feature/column to use as event time
   * @param features the list of feature objects if defined explicitly
   * @param statisticsConfig the statistics configuration for the feature group
   *
   * @return The feature group metadata object.
   */
  public FeatureGroupBase createStreamFeatureGroup(@NonNull String name,
                                                            Integer version,
                                                            String description,
                                                            Boolean onlineEnabled,
                                                            TimeTravelFormat timeTravelFormat,
                                                            List<String> primaryKeys,
                                                            List<String> partitionKeys,
                                                            String eventTime,
                                                            List<Feature> features,
                                                            StatisticsConfig statisticsConfig) {
    return createStreamFeatureGroup(name,
        version,
        description,
        onlineEnabled,
        timeTravelFormat,
        primaryKeys,
        partitionKeys,
        eventTime,
        null,
        features,
        statisticsConfig,
        null,
        null);
  }


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
