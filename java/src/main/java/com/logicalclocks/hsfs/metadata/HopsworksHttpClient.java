/*
 * Copyright (c) 2020 Logical Clocks AB
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

package com.logicalclocks.hsfs.metadata;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Project;
import org.apache.http.HttpRequest;
import org.apache.http.client.ResponseHandler;

import java.io.IOException;

public interface HopsworksHttpClient {
  <T> T handleRequest(HttpRequest request, ResponseHandler<T> responseHandler)
      throws IOException, FeatureStoreException;

  String downloadCredentials(Project project, String certPath) throws IOException, FeatureStoreException;
}
