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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class EngineBase {

  protected static final Logger LOGGER = LoggerFactory.getLogger(EngineBase.class);

  public String addFile(String filePath) {
    return null; // todo needs fixing
  }

  public Map<String, String> getKafkaConfig(FeatureGroupBase featureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    boolean external = !System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS)
        && (writeOptions == null
          || !Boolean.parseBoolean(writeOptions.getOrDefault("internal_kafka", "false")));

    StorageConnector.KafkaConnector storageConnector = featureGroup.getFeatureStore().getKafkaConnector(this, external);

    Map<String, String> config = this.getClass().getName().endsWith("SparkEngine")
        ? storageConnector.sparkOptions() : storageConnector.kafkaOptions();

    if (writeOptions != null) {
      config.putAll(writeOptions);
    }
    return config;
  }
}
