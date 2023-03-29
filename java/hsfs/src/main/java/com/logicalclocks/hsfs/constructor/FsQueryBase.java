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

package com.logicalclocks.hsfs.constructor;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class FsQueryBase<T extends FeatureGroupBase, T2 extends FeatureGroupBase> {
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
  private List<FeatureGroupAlias<T2>> onDemandFeatureGroups;

  @Getter
  @Setter
  private List<FeatureGroupAlias<T>> hudiCachedFeatureGroups;

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

  public abstract void registerOnDemandFeatureGroups() throws FeatureStoreException, IOException;

  public abstract void registerHudiFeatureGroups(Map<String, String> readOptions) throws FeatureStoreException;
}
