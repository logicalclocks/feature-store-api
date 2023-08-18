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

package com.logicalclocks.hsfs.beam.engine;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.beam.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.EngineBase;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BeamEngine extends EngineBase {
  private static BeamEngine INSTANCE = null;

  public static synchronized BeamEngine getInstance() throws FeatureStoreException {
    if (INSTANCE == null) {
      INSTANCE = new BeamEngine();
    }
    return INSTANCE;
  }

  private BeamEngine() throws FeatureStoreException {
  }

  public BeamProducer insertStream(StreamFeatureGroup streamFeatureGroup, Map<String, String> writeOptions)
      throws FeatureStoreException, IOException {
    Map<String, Schema> complexFeatureSchemas = new HashMap<>();
    for (String featureName: streamFeatureGroup.getComplexFeatures()) {
      complexFeatureSchemas.put(featureName,
            new Schema.Parser().parse(streamFeatureGroup.getFeatureAvroSchema(featureName)));
    }
    Schema deserializedEncodedSchema = new Schema.Parser().parse(streamFeatureGroup.getEncodedAvroSchema());

    return new BeamProducer(streamFeatureGroup.getOnlineTopicName(),
      getKafkaConfig(streamFeatureGroup, writeOptions),
      streamFeatureGroup.getDeserializedAvroSchema(), deserializedEncodedSchema, complexFeatureSchemas,
      streamFeatureGroup.getPrimaryKeys());
  }

  @Override
  public String addFile(String filePath) throws IOException {
    if (Strings.isNullOrEmpty(filePath)) {
      return filePath;
    }
    // this is used for unit testing
    if (!filePath.startsWith("file://")) {
      filePath = "hdfs://" + filePath;
    }
    String targetPath = filePath.substring(filePath.lastIndexOf("/") + 1);
    FileUtils.copyFile(new File(filePath), new File(targetPath));
    return targetPath;
  }
}
