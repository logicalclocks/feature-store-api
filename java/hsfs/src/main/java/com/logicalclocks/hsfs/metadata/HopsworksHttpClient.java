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

package com.logicalclocks.hsfs.metadata;

import com.logicalclocks.hsfs.FeatureStoreException;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpRequest;
import org.apache.http.client.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public interface HopsworksHttpClient {
  Logger LOGGER = LoggerFactory.getLogger(HopsworksHttpClient.class);

  <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler)
      throws IOException, FeatureStoreException;

  String getTrustStorePath();

  void setTrustStorePath(String trustStorePath);

  String getKeyStorePath();

  void setKeyStorePath(String keyStorePath);

  String getCertKey();

  void setCertKey(String certKey);

  static String readCertKey(String materialPwd) {
    try {
      return FileUtils.readFileToString(new File(materialPwd), Charset.defaultCharset());
    } catch (IOException ex) {
      LOGGER.warn("Failed to get cert password.", ex);
    }
    return null;
  }
}
