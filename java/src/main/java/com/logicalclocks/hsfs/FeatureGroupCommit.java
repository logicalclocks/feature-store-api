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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureGroupCommit {
  @Getter @Setter
  private Integer commitID;
  @Getter @Setter
  private Date committedOn;
  @Getter @Setter
  private Long rowsInserted;
  @Getter @Setter
  private Long rowsUpdated;
  @Getter @Setter
  private Long rowsDeleted;

  @Builder
  public FeatureGroupCommit(Integer commitID, Date committedOn, Long rowsInserted, Long rowsUpdated, Long rowsDeleted) {

    this.commitID = commitID;
    this.committedOn = committedOn;
    this.rowsInserted = rowsInserted;
    this.rowsUpdated = rowsUpdated;
    this.rowsDeleted = rowsDeleted;
  }

  public FeatureGroupCommit() {
  }

}
