/*
 *  Copyright (c) 2022. Logical Clocks AB
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

import org.apache.spark.sql.streaming.StreamingQuery;

public class HsfsSparkStream implements HsfsStream {

  private StreamingQuery streamingQuery;

  <S extends StreamingQuery> HsfsSparkStream(S s) {
    streamingQuery = (StreamingQuery) s;
  }

  public HsfsSparkStream() {
  }

  @Override
  public void setStream(Object stream) {
    this.streamingQuery = (StreamingQuery) stream;
  }

  @Override
  public Object getStream() {
    return streamingQuery;
  }
}
