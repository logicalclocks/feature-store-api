/*
 *  Copyright (c) 2023. Hopsworks AB
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

package com.logicalclocks.hsfs.flink;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.flink.constructor.Query;
import com.logicalclocks.hsfs.flink.engine.FeatureViewEngine;
import com.logicalclocks.hsfs.flink.engine.FeatureGroupEngine;

import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;

public class FeatureStore extends FeatureStoreBase<Query> {

  private FeatureGroupEngine featureGroupEngine;
  private FeatureViewEngine featureViewEngine;

  public FeatureStore() {
    featureViewEngine = new FeatureViewEngine();
    featureGroupEngine = new FeatureGroupEngine();
    storageConnectorApi = new StorageConnectorApi();
  }

  /**
   * Create a feature group builder object.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *
   *        // create feature group metadata object
   *        StreamFeatureGroup streamFeatureGroup = fs.createStreamFeatureGroup()
   *               .name("documentation_example")
   *               .version(1)
   *               .primaryKeys(Collections.singletonList("pk"))
   *               .eventTime("event_time")
   *               .onlineEnabled(true)
   *               .features(features)
   *               .build();
   *
   *         // save the feature group metadata object on the feature store
   *         streamFeatureGroup.save()
   * }
   * </pre>
   *
   * @return StreamFeatureGroup.StreamFeatureGroupBuilder a StreamFeatureGroup builder object.
   */
  public StreamFeatureGroup.StreamFeatureGroupBuilder createStreamFeatureGroup() {
    return StreamFeatureGroup.builder().featureStore(this);
  }

  @Override
  public StreamFeatureGroup createStreamFeatureGroup(@NonNull String name, Integer version, String description,
                                                     Boolean onlineEnabled, TimeTravelFormat timeTravelFormat,
                                                     List<String> primaryKeys, List<String> partitionKeys,
                                                     String eventTime, String hudiPrecombineKey, List<Feature> features,
                                                     StatisticsConfig statisticsConfig,
                                                     StorageConnector storageConnector, String path) {
    return new StreamFeatureGroup.StreamFeatureGroupBuilder()
        .featureStore(this)
        .name(name)
        .version(version)
        .description(description)
        .onlineEnabled(onlineEnabled)
        .timeTravelFormat(timeTravelFormat)
        .primaryKeys(primaryKeys)
        .partitionKeys(partitionKeys)
        .eventTime(eventTime)
        .hudiPrecombineKey(hudiPrecombineKey)
        .features(features)
        .statisticsConfig(statisticsConfig)
        .storageConnector(storageConnector)
        .path(path)
        .build();
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(@NonNull String name,
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
                                                          String path) throws IOException, FeatureStoreException {

    return featureGroupEngine.getOrCreateFeatureGroup(this, name, version, description, onlineEnabled,
        timeTravelFormat, primaryKeys, partitionKeys, eventTime, hudiPrecombineKey, features, statisticsConfig,
        storageConnector, path);
  }

  /**
   * Get a stream feature group object from the feature store.
   *
   * <p>Getting a stream feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getStreamFeatureGroup("electricity_prices", 1);
   * }
   * </pre>
   *
   * @param name the name of the feature group
   * @return StreamFeatureGroup The stream feature group metadata object.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getStreamFeatureGroup(name, DEFAULT_VERSION);
  }

  /**
   * Get a stream feature group object from the feature store.
   *
   * <p>Getting a stream feature group metadata handle enables to interact with the feature group,
   * such as read the data or use the `Query`-API to perform joins between feature groups and create feature
   * views.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureGroup fg = fs.getStreamFeatureGroup("electricity_prices", 1);
   * }
   * </pre>
   *
   * @param name the name of the feature group
   * @param version the version of the feature group
   * @return StreamFeatureGroup The stream feature group metadata object.
   * @throws FeatureStoreException If unable to retrieve feature group from the feature store.
   * @throws IOException Generic IO exception.
   */
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureGroupEngine.getStreamFeatureGroup(this, name, version);
  }

  /**
   * Get a feature view object from the selected feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   * }
   * </pre>
   *
   * @param name    Name of the feature view.
   * @param version Version to get.
   * @return FeatureView The feature view metadata object.
   * @throws FeatureStoreException If unable to retrieve FeatureView from the feature store.
   * @throws IOException Generic IO exception.
   */
  public FeatureView getFeatureView(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureViewEngine.get(this, name, version);
  }

  /**
   * Get a feature view object from the selected feature store.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *        FeatureView fv = fs.getFeatureView("fv_name", 1);
   * }
   * </pre>
   *
   * @param name    Name of the feature view.
   * @return FeatureView The feature view metadata object.
   * @throws FeatureStoreException If unable to retrieve FeatureView from the feature store.
   * @throws IOException Generic IO exception.
   */
  public FeatureView getFeatureView(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature view `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getFeatureView(name, DEFAULT_VERSION);
  }
}
