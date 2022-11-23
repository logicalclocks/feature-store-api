/*
 *  Copyright (c) 2021-2022. Hopsworks AB
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

package com.logicalclocks.hsfs.engine.hudi;

import com.logicalclocks.base.metadata.FeatureStoreApi;
import com.logicalclocks.hsfs.engine.SparkEngine;
import lombok.SneakyThrows;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DeltaStreamerTransformer implements Transformer {

  private final FeatureStoreApi featureStoreApi = new FeatureStoreApi();

  public DeltaStreamerTransformer() {
  }

  @SneakyThrows
  @Override
  public Dataset<Row> apply(JavaSparkContext javaSparkContext, SparkSession sparkSession, Dataset<Row> dataset,
                            TypedProperties props) {
    return SparkEngine.getInstance().convertToDefaultDataframe(dataset);
  }
}
