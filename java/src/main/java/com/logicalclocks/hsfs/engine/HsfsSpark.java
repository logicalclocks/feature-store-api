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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HsfsSpark implements HsfsDataSet {

  private Dataset<Row> data;

  <S extends Dataset<Row>> HsfsSpark(S s) {
    data = s.toDF();
  }

  public HsfsSpark() {
  }

  @Override
  public void setDataSet(Object data) {
    this.data = (Dataset<Row>) data;
  }

  @Override
  public Dataset<Row> getDataSet() {
    return data;
  }
}
