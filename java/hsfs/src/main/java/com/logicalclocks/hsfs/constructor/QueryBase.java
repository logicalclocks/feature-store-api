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

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.QueryConstructorApi;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public abstract class QueryBase<T extends QueryBase<T, T2, T3>, T2 extends FeatureGroupBase, T3> {

  @Getter
  protected FeatureGroupBase leftFeatureGroup;
  @Getter
  @Setter
  protected List<Feature> leftFeatures;
  @Getter
  @Setter
  protected Long leftFeatureGroupStartTime;
  @Getter
  @Setter
  protected Long leftFeatureGroupEndTime;
  @Getter
  @Setter
  protected List<Join<T>> joins = new ArrayList<>();
  @Getter
  @Setter
  protected FilterLogic filter;
  @Getter
  @Setter
  protected Boolean hiveEngine = false;

  protected void setLeftFeatureGroup(T2 leftFeatureGroup) {
    setLeftBaseFeatureGroup(leftFeatureGroup);
  }

  private void setLeftBaseFeatureGroup(FeatureGroupBase leftFeatureGroup) {
    this.leftFeatureGroup = leftFeatureGroup;
  }

  protected static final Logger LOGGER = LoggerFactory.getLogger(QueryBase.class);

  protected QueryConstructorApi queryConstructorApi = new QueryConstructorApi();
  protected StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private FeatureGroupUtils utils = new FeatureGroupUtils();

  protected QueryBase(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    setLeftBaseFeatureGroup(leftFeatureGroup);
    this.leftFeatures = addFeatureGroupToFeatures(leftFeatureGroup, leftFeatures);
  }

  public abstract String sql();

  public abstract String sql(Storage storage);

  public <T2> String sql(Storage storage, Class<T2> fsQueryType) {
    try {
      return queryConstructorApi
          .constructQuery(this.getLeftFeatureGroup().getFeatureStore(), this, fsQueryType)
          .getStorageQuery(storage);
    } catch (FeatureStoreException | IOException e) {
      return e.getMessage();
    }
  }

  public T join(T subquery) {
    return join(subquery, JoinType.INNER);
  }

  public T join(T subquery, String prefix) {
    return join(subquery, JoinType.INNER, prefix);
  }

  public T join(T subquery, List<String> on) {
    return joinFeatures(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public T join(T subquery, List<String> leftOn, List<String> rightOn) {
    return joinFeatures(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public T join(T subquery, List<String> leftOn, List<String> rightOn, String prefix) {
    return joinFeatures(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER, prefix);
  }

  public T join(T subquery, JoinType joinType) {
    joins.add(new Join(subquery, joinType, null));
    return (T)this;
  }

  public T join(T subquery, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, joinType, prefix));
    return (T)this;
  }

  public T join(T subquery, List<String> on, JoinType joinType) {
    joins.add(new Join(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), joinType, null));
    return (T)this;
  }

  public T join(T subquery, List<String> on, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), joinType, prefix));
    return (T)this;
  }

  public T join(T subquery, List<String> leftOn, List<String> rightOn, JoinType joinType) {
    joins.add(new Join(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), joinType, null));
    return (T)this;
  }

  public T join(T subquery, List<String> leftOn, List<String> rightOn, JoinType joinType,
                               String prefix) {
    joins.add(new Join(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), joinType, prefix));
    return (T)this;
  }

  public T joinFeatures(T subquery, List<Feature> on) {
    return joinFeatures(subquery, on, JoinType.INNER);
  }

  public T joinFeatures(T subquery, List<Feature> on, String prefix) {
    return joinFeatures(subquery, on, JoinType.INNER, prefix);
  }

  public T joinFeatures(T subquery, List<Feature> leftOn, List<Feature> rightOn) {
    return joinFeatures(subquery, leftOn, rightOn, JoinType.INNER);
  }

  public T joinFeatures(T subquery, List<Feature> leftOn, List<Feature> rightOn, String prefix) {
    return joinFeatures(subquery, leftOn, rightOn, JoinType.INNER, prefix);
  }

  public T joinFeatures(T subquery, List<Feature> on, JoinType joinType) {
    joins.add(new Join(subquery, on, joinType, null));
    return (T)this;
  }

  public T joinFeatures(T subquery, List<Feature> on, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, on, joinType, prefix));
    return (T)this;
  }

  public T joinFeatures(T subquery, List<Feature> leftOn, List<Feature> rightOn,
                                       JoinType joinType) {
    joins.add(new Join(subquery, leftOn, rightOn, joinType, null));
    return (T)this;
  }

  public T joinFeatures(T subquery, List<Feature> leftOn, List<Feature> rightOn,
                                       JoinType joinType, String prefix) {
    joins.add(new Join(subquery, leftOn, rightOn, joinType, prefix));
    return (T)this;
  }

  /**
   * Perform time travel on the given Query.
   * This method returns a new Query object at the specified point in time.
   * This can then either be read into a Dataframe or used further to perform joins
   * or construct a training dataset.
   *
   * @param wallclockTime point in time
   * @return Query
   * @throws FeatureStoreException
   * @throws ParseException
   */
  public T asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return asOf(wallclockTime, null);
  }

  /**
   * Perform time travel on the given Query.
   * This method returns a new Query object at the specified point in time.
   * This can then either be read into a Dataframe or used further to perform joins
   * or construct a training dataset.
   *
   * @param wallclockTime point in time
   * @param excludeUntil point in time
   * @return Query
   * @throws FeatureStoreException
   * @throws ParseException
   */
  public T asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    Long wallclockTimestamp = utils.getTimeStampFromDateString(wallclockTime);
    Long excludeUntilTimestamp = null;
    if (excludeUntil != null) {
      excludeUntilTimestamp = utils.getTimeStampFromDateString(excludeUntil);
    }
    for (Join join : this.joins) {
      QueryBase queryBaseWithTimeStamp = join.getQuery();
      queryBaseWithTimeStamp.setLeftFeatureGroupEndTime(wallclockTimestamp);
      if (excludeUntilTimestamp != null) {
        queryBaseWithTimeStamp.setLeftFeatureGroupStartTime(excludeUntilTimestamp);
      }
      join.setQuery(queryBaseWithTimeStamp);
    }
    this.setLeftFeatureGroupEndTime(wallclockTimestamp);
    if (excludeUntilTimestamp != null) {
      this.setLeftFeatureGroupStartTime(excludeUntilTimestamp);
    }
    return (T)this;
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return Query
   * @throws FeatureStoreException
   * @throws ParseException
   *
   * @deprecated use asOf(wallclockEndTime, wallclockStartTime) instead
   */
  public T pullChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, ParseException {
    this.setLeftFeatureGroupStartTime(utils.getTimeStampFromDateString(wallclockStartTime));
    this.setLeftFeatureGroupEndTime(utils.getTimeStampFromDateString(wallclockEndTime));
    return (T)this;
  }

  public T filter(Filter filter) {
    if (this.filter == null) {
      this.filter = new FilterLogic(filter);
    } else {
      this.filter = this.filter.and(filter);
    }
    return (T)this;
  }

  public T filter(FilterLogic filter) {
    if (this.filter == null) {
      this.filter = filter;
    } else {
      this.filter = this.filter.and(filter);
    }
    return (T)this;
  }

  public T appendFeature(Feature feature) {
    this.leftFeatures.add(feature);
    return (T)this;
  }

  @JsonIgnore
  public boolean isTimeTravel() {
    if (leftFeatureGroupStartTime != null || leftFeatureGroupEndTime != null) {
      return true;
    }
    for (Join join: joins) {
      if (join.getQuery().isTimeTravel()) {
        return true;
      }
    }
    return false;
  }

  private List<Feature> addFeatureGroupToFeatures(FeatureGroupBase featureGroupBase, List<Feature> leftFeatures) {
    List<Feature> updatedFeatures = new ArrayList<>();
    for (Feature feature: leftFeatures) {
      feature.setFeatureGroupId(featureGroupBase.getId());
      updatedFeatures.add(feature);
    }
    return updatedFeatures;
  }

  public abstract Object read()
      throws FeatureStoreException, IOException;

  public abstract Object read(boolean online)
      throws FeatureStoreException, IOException;

  public abstract Object read(boolean online, Map<String, String> readOptions)
      throws FeatureStoreException, IOException;

  public abstract void show(int numRows) throws FeatureStoreException, IOException;

  public abstract void show(boolean online, int numRows) throws FeatureStoreException, IOException;
}
