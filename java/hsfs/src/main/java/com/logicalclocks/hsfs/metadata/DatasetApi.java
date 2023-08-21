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

package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class DatasetApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetApi.class.getName());

  public DatasetApi()  {
  }

  public static String downloadHdfsPath(Integer projectId,String path, String datasetType) throws FeatureStoreException,
      IOException {
    if (projectId == null) {
      throw new  FeatureStoreException("Project Id cannot be null for reading HDFS file path.");
    }
    if (Strings.isNullOrEmpty(datasetType)) {
      datasetType = "DATASET";
    }
    StringBuilder pathBuilder = new StringBuilder()
        .append(HopsworksClient.PROJECT_PATH)
        .append("/dataset/download/with_auth")
        .append("{/path}")
        .append("{?type}");

    UriTemplate uri = UriTemplate.fromTemplate(pathBuilder.toString())
        .set("projectId", projectId)
        .set("path",path)
        .set("type",datasetType);
    String uriString = uri.expand();

    return HopsworksClient.getInstance().handleRequest(new HttpGet(uriString), new BasicResponseHandler());
  }
}
