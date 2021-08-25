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

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.metadata.Code;
import com.logicalclocks.hsfs.metadata.CodeApi;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CodeEngine {

  private CodeApi codeApi;
  private static final String KERNEL_ENV = "HOPSWORKS_KERNEL_ID";
  private static final String WEB_PROXY_ENV = "APPLICATION_WEB_PROXY_BASE";

  private static final Logger LOGGER = LoggerFactory.getLogger(CodeEngine.class);

  public CodeEngine(EntityEndpointType entityType) {
    this.codeApi = new CodeApi(entityType);
  }

  public Code saveCode(TrainingDataset trainingDataset)
          throws FeatureStoreException, IOException {
    String kernelId = System.getenv(KERNEL_ENV);
    if (Strings.isNullOrEmpty(kernelId)) {
      return null;
    }
    return codeApi.post(trainingDataset, saveCode(),
            kernelId, Code.RunType.JUPYTER);
  }

  public Code saveCode(FeatureGroupBase featureGroup)
          throws FeatureStoreException, IOException {
    String kernelId = System.getenv(KERNEL_ENV);
    if (Strings.isNullOrEmpty(kernelId)) {
      return null;
    }
    return codeApi.post(featureGroup, saveCode(),
            kernelId, Code.RunType.JUPYTER);
  }

  private Code saveCode() {
    Long commitTime = Timestamp.valueOf(LocalDateTime.now()).getTime();
    String applicationId = null;
    String webProxy = System.getenv(WEB_PROXY_ENV);
    if (!Strings.isNullOrEmpty(webProxy)) {
      applicationId = webProxy.substring(7);
    }
    return new Code(commitTime, applicationId);
  }
}
