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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.Query;
import com.logicalclocks.hsfs.util.Constants;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroupCommit {
  @Getter @Setter
  private Integer commitID;
  @Getter @Setter
  private Date commitTimestamp;
  @Getter @Setter
  private Long rowsInserted;
  @Getter @Setter
  private Long rowsUpdated;
  @Getter @Setter
  private Long rowsDeleted;
  @Getter @Setter
  private String commitMessage;

  @Builder
  public FeatureGroupCommit(Integer commitID, Date commitTimestamp, Long rowsInserted, Long rowsUpdated,
                            Long rowsDeleted, String commitMessage)
      throws FeatureStoreException {

    this.commitID = commitID;
    this.commitTimestamp = commitTimestamp;
    this.rowsInserted = rowsInserted;
    this.rowsUpdated = rowsUpdated;
    this.rowsDeleted = rowsDeleted;
    this.commitMessage = commitMessage;
  }

  public FeatureGroupCommit() {
  }

}
