/*
 * Copyright (c) 2021. Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.metadata.KafkaApi;
import com.logicalclocks.hsfs.metadata.StreamFeatureGroupApi;
import com.logicalclocks.hsfs.metadata.validation.ValidationType;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class StreamFeatureGroupEngine {

  private Utils utils = new Utils();
  private KafkaApi kafkaApi = new KafkaApi();
  private StreamFeatureGroupApi streamFeatureGroupApi = new StreamFeatureGroupApi();

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);


  public <T> Object insertStream(StreamFeatureGroup streamFeatureGroup, T featureData, String queryName,
                              String outputMode, boolean awaitTermination, Long timeout,
                              Map<String, String> writeOptions)
            throws FeatureStoreException, IOException, StreamingQueryException, TimeoutException {

    if (streamFeatureGroup.getValidationType() != ValidationType.NONE) {
      LOGGER.info("ValidationWarning: Stream ingestion for feature group `" + streamFeatureGroup.getName()
                    + "`, with version `" + streamFeatureGroup.getVersion() + "` will not perform validation.");
    }

    // start streaming to hudi table from online feature group topic
    if (streamFeatureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI) {
      writeOptions.put("functionType", "streamingQuery");
      streamFeatureGroupApi.deltaStreamerJob(streamFeatureGroup, writeOptions);
    } else {
      throw new FeatureStoreException("Hudi DeltaStreamer is only supported for Hudi time travel enabled feature "
                    + "groups");
    }

    return SparkEngine.getInstance().writeStreamDataframe(streamFeatureGroup,
      utils.sanitizeFeatureNames(featureData), queryName, outputMode, awaitTermination, timeout,
      utils.getKafkaConfig(streamFeatureGroup, writeOptions));
  }

  public String getAvroSchema(StreamFeatureGroup featureGroup) throws FeatureStoreException, IOException {
    return kafkaApi.getTopicSubject(featureGroup.getFeatureStore(), featureGroup.getOnlineTopicName()).getSchema();
  }
}
