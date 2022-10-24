/*
 *  Copyright (c) 2020-2022. Hopsworks AB
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

package com.logicalclocks.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public abstract class HopsworksConnectionBase implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HopsworksConnectionBase.class);

  public abstract Object getFeatureStore(String name) throws IOException, FeatureStoreException;

  public abstract String rewriteFeatureStoreName(String name);

  /**
   * Close the connection and clean up the certificates.
   */
  public void close() {
    // Close the client
  }

  public abstract Project getProject() throws IOException, FeatureStoreException;

  public abstract String getProjectName(String project);
}
