/*
 * Copyright (c) 2021 Logical Clocks AB
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

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.metadata.Code;
import com.logicalclocks.hsfs.metadata.CodeApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

public class CodeEngine {

  private CodeApi codeApi;

  private static final Logger LOGGER = LoggerFactory.getLogger(CodeEngine.class);

  public CodeEngine(EntityEndpointType entityType) {
    this.codeApi = new CodeApi(entityType);
  }

  public Code computeCode(TrainingDataset trainingDataset, Dataset<Row> dataFrame)
          throws FeatureStoreException, IOException {
    return codeApi.post(trainingDataset, computeCode(dataFrame,
            trainingDataset.getStatisticsConfig().getColumns(),
            trainingDataset.getStatisticsConfig().getHistograms(),
            trainingDataset.getStatisticsConfig().getCorrelations(),
            null), System.getenv("HOPSWORKS_KERNEL_ID"), "JUPYTER");
  }

  public Code computeCode(FeatureGroupBase featureGroup, Dataset<Row> dataFrame, Long commitId)
          throws FeatureStoreException, IOException {
    return codeApi.post(featureGroup, computeCode(dataFrame,
            featureGroup.getStatisticsConfig().getColumns(),
            featureGroup.getStatisticsConfig().getHistograms(),
            featureGroup.getStatisticsConfig().getCorrelations(),
            commitId), System.getenv("HOPSWORKS_KERNEL_ID"), "JUPYTER");
  }

  private Code computeCode(Dataset<Row> dataFrame, List<String> statisticColumns, Boolean histograms,
                           Boolean correlations, Long commitId) throws FeatureStoreException {
    if (dataFrame.isEmpty()) {
      throw new FeatureStoreException("There is no data in the entity that you are trying to compute statistics for. A "
              + "possible cause might be that you inserted only data to the online storage of a feature group.");
    }
    Long commitTime = Timestamp.valueOf(LocalDateTime.now()).getTime();
    String content = SparkEngine.getInstance().profile(dataFrame, statisticColumns, histograms, correlations);
    return new Code(commitTime, commitId, content, System.getenv("APPLICATION_WEB_PROXY_BASE").substring(7));
  }
}
