/*
 *  Copyright (c) 2021-2023. Hopsworks AB
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

import com.databricks.dbutils_v1.DBUtilsV1;
import com.databricks.dbutils_v1.DBUtilsHolder;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.metadata.Code;
import com.logicalclocks.hsfs.metadata.CodeApi;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDatasetBase;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CodeEngine {

  private CodeApi codeApi;

  private static final String WEB_PROXY_ENV = "APPLICATION_WEB_PROXY_BASE";

  //JUPYTER
  private static final String KERNEL_ENV = "HOPSWORKS_KERNEL_ID";

  //JOB
  private static final String JOB_ENV = "HOPSWORKS_JOB_NAME";

  public CodeEngine(EntityEndpointType entityType) {
    this.codeApi = new CodeApi(entityType);
  }

  public void saveCode(TrainingDatasetBase trainingDatasetBase) throws FeatureStoreException, IOException {
    String kernelId = System.getenv(KERNEL_ENV);
    String jobName = System.getenv(JOB_ENV);

    if (!Strings.isNullOrEmpty(kernelId)) {
      codeApi.post(trainingDatasetBase, saveCode(), kernelId, Code.RunType.JUPYTER, null);
    } else if (!Strings.isNullOrEmpty(jobName)) {
      codeApi.post(trainingDatasetBase, saveCode(), jobName, Code.RunType.JOB, null);
    } else {
      try {
        DBUtilsV1 dbutils = DBUtilsHolder.dbutils();
        String notebookPath = dbutils.notebook().getContext().notebookPath().get();
        String browserHostName = dbutils.notebook().getContext().browserHostName().get();
        codeApi.post(trainingDatasetBase, saveCode(), notebookPath, Code.RunType.DATABRICKS, browserHostName);
      } catch (Exception e) {
        // ignore the exception - the code might be running on a third party platform where databricks
        // library is not available
      }
    }
  }

  public void saveCode(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    String kernelId = System.getenv(KERNEL_ENV);
    String jobName = System.getenv(JOB_ENV);

    if (!Strings.isNullOrEmpty(kernelId)) {
      codeApi.post(featureGroup, saveCode(), kernelId, Code.RunType.JUPYTER, null);
    } else if (!Strings.isNullOrEmpty(jobName)) {
      codeApi.post(featureGroup, saveCode(), jobName, Code.RunType.JOB, null);
    } else {
      try {
        DBUtilsV1 dbutils = DBUtilsHolder.dbutils();
        String notebookPath = dbutils.notebook().getContext().notebookPath().get();
        String browserHostName = dbutils.notebook().getContext().browserHostName().get();
        codeApi.post(featureGroup, saveCode(), notebookPath, Code.RunType.DATABRICKS, browserHostName);
      } catch (Throwable e) {
        // ignore the exception - the code might be running on a third party platform where databricks
        // library is not available
        // Throwable because the "class not found" is an error not exception
      }
    }
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
