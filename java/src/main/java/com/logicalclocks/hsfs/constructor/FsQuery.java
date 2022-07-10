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

package com.logicalclocks.hsfs.constructor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.OnDemandFeatureGroup;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
public class FsQuery {
  @Getter
  @Setter
  private String query;

  @Getter
  @Setter
  private String queryOnline;

  @Getter
  @Setter
  private String pitQuery;

  @Getter
  @Setter
  private List<OnDemandFeatureGroupAlias> onDemandFeatureGroups;

  @Getter
  @Setter
  private List<HudiFeatureGroupAlias> hudiCachedFeatureGroups;

  public void removeNewLines() {
    query = query != null ? query.replace("\n", " ") : null;
    queryOnline = queryOnline != null ? queryOnline.replace("\n", " ") : null;
  }

  public String getStorageQuery(Storage storage) throws FeatureStoreException {
    switch (storage) {
      case OFFLINE:
        if (!Strings.isNullOrEmpty(pitQuery)) {
          return pitQuery;
        }
        return query;
      case ONLINE:
        return queryOnline;
      default:
        throw new FeatureStoreException("Cannot run query on ALL storages");
    }
  }

  public void registerOnDemandFeatureGroups() throws FeatureStoreException, IOException {
    if (onDemandFeatureGroups == null || onDemandFeatureGroups.isEmpty()) {
      return;
    }

    for (OnDemandFeatureGroupAlias onDemandFeatureGroupAlias : onDemandFeatureGroups) {
      String alias = onDemandFeatureGroupAlias.getAlias();
      OnDemandFeatureGroup onDemandFeatureGroup = onDemandFeatureGroupAlias.getOnDemandFeatureGroup();

      SparkEngine.getInstance().registerOnDemandTemporaryTable(onDemandFeatureGroup, alias);
    }
  }

  public void registerHudiFeatureGroups(Map<String, String> readOptions) {
    for (HudiFeatureGroupAlias hudiFeatureGroupAlias : hudiCachedFeatureGroups) {
      SparkEngine.getInstance().registerHudiTemporaryTable(hudiFeatureGroupAlias, readOptions);
    }
  }
}
