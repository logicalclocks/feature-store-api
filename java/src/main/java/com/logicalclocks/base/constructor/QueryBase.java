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

package com.logicalclocks.base.constructor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.logicalclocks.base.Feature;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.Storage;
import com.logicalclocks.base.engine.FeatureGroupUtils;
import com.logicalclocks.base.metadata.FeatureGroupBase;

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
public class QueryBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryBase.class);

  @Getter
  @Setter
  private FeatureGroupBase leftFeatureGroup;
  @Getter
  @Setter
  private List<Feature> leftFeatures;
  @Getter
  @Setter
  private Long leftFeatureGroupStartTime;
  @Getter
  @Setter
  private Long leftFeatureGroupEndTime;
  @Getter
  @Setter
  private List<Join> joins = new ArrayList<>();
  @Getter
  @Setter
  private FilterLogic filter;
  @Getter
  @Setter
  private Boolean hiveEngine = false;

  private FeatureGroupUtils utils = new FeatureGroupUtils();

  public QueryBase(FeatureGroupBase leftFeatureGroup, List<Feature> leftFeatures) {
    this.leftFeatureGroup = leftFeatureGroup;
    this.leftFeatures = leftFeatures;
  }

  public String sql() {
    return null;
  }

  public String sql(Storage storage) {
    return null;
  }

  public QueryBase genericJoin(QueryBase subquery) {
    return genericJoin(subquery, JoinType.INNER);
  }

  public QueryBase genericJoin(QueryBase subquery, String prefix) {
    return genericJoin(subquery, JoinType.INNER, prefix);
  }

  public QueryBase genericJoin(QueryBase subquery, List<String> on) {
    return genericJoinFeatures(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public QueryBase genericJoin(QueryBase subquery, List<String> leftOn, List<String> rightOn) {
    return genericJoinFeatures(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public QueryBase genericJoin(QueryBase subquery, List<String> leftOn, List<String> rightOn, String prefix) {
    return genericJoinFeatures(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER, prefix);
  }

  public QueryBase genericJoin(QueryBase subquery, JoinType joinType) {
    joins.add(new Join(subquery, joinType, null));
    return this;
  }

  public QueryBase genericJoin(QueryBase subquery, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, joinType, prefix));
    return this;
  }

  public QueryBase genericJoin(QueryBase subquery, List<String> on, JoinType joinType) {
    joins.add(new Join(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), joinType, null));
    return this;
  }

  public QueryBase genericJoin(QueryBase subquery, List<String> on, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), joinType, prefix));
    return this;
  }

  public QueryBase genericJoin(QueryBase subquery, List<String> leftOn, List<String> rightOn, JoinType joinType) {
    joins.add(new Join(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), joinType, null));
    return this;
  }

  public QueryBase genericJoin(QueryBase subquery, List<String> leftOn, List<String> rightOn, JoinType joinType,
                               String prefix) {
    joins.add(new Join(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), joinType, prefix));
    return this;
  }

  public QueryBase genericJoinFeatures(QueryBase subquery, List<Feature> on) {
    return genericJoinFeatures(subquery, on, JoinType.INNER);
  }

  public QueryBase genericJoinFeatures(QueryBase subquery, List<Feature> on, String prefix) {
    return genericJoinFeatures(subquery, on, JoinType.INNER, prefix);
  }

  public QueryBase genericJoinFeatures(QueryBase subquery, List<Feature> leftOn, List<Feature> rightOn) {
    return genericJoinFeatures(subquery, leftOn, rightOn, JoinType.INNER);
  }

  public QueryBase genericJoinFeatures(QueryBase subquery, List<Feature> leftOn, List<Feature> rightOn, String prefix) {
    return genericJoinFeatures(subquery, leftOn, rightOn, JoinType.INNER, prefix);
  }

  public QueryBase genericJoinFeatures(QueryBase subquery, List<Feature> on, JoinType joinType) {
    joins.add(new Join(subquery, on, joinType, null));
    return this;
  }

  public QueryBase genericJoinFeatures(QueryBase subquery, List<Feature> on, JoinType joinType, String prefix) {
    joins.add(new Join(subquery, on, joinType, prefix));
    return this;
  }

  public QueryBase genericJoinFeatures(QueryBase subquery, List<Feature> leftOn, List<Feature> rightOn,
                                       JoinType joinType) {
    joins.add(new Join(subquery, leftOn, rightOn, joinType, null));
    return this;
  }

  public QueryBase genericJoinFeatures(QueryBase subquery, List<Feature> leftOn, List<Feature> rightOn,
                                       JoinType joinType, String prefix) {
    joins.add(new Join(subquery, leftOn, rightOn, joinType, prefix));
    return this;
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
  public QueryBase genericAsOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return genericAsOf(wallclockTime, null);
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
  public QueryBase genericAsOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
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
    return this;
  }

  /**
   * Reads changes that occurred between specified points in time.
   *
   * @param wallclockStartTime start date.
   * @param wallclockEndTime   end date.
   * @return Query
   * @throws FeatureStoreException
   * @throws IOException
   * @throws ParseException
   *
   * @deprecated use asOf(wallclockEndTime, wallclockStartTime) instead
   */
  public QueryBase genericPullChanges(String wallclockStartTime, String wallclockEndTime)
      throws FeatureStoreException, ParseException {
    this.setLeftFeatureGroupStartTime(utils.getTimeStampFromDateString(wallclockStartTime));
    this.setLeftFeatureGroupEndTime(utils.getTimeStampFromDateString(wallclockEndTime));
    return this;
  }

  public QueryBase genericFilter(Filter filter) {
    if (this.filter == null) {
      this.filter = new FilterLogic(filter);
    } else {
      this.filter = this.filter.and(filter);
    }
    return this;
  }

  public QueryBase genericFilter(FilterLogic filter) {
    if (this.filter == null) {
      this.filter = filter;
    } else {
      this.filter = this.filter.and(filter);
    }
    return this;
  }

  public QueryBase getericAppendFeature(Feature feature) {
    this.leftFeatures.add(feature);
    return this;
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

  public Object read(boolean online, Map<String, String> readOptions) throws FeatureStoreException,
      IOException {
    return null;
  }

  public void show(boolean online, int numRows) throws FeatureStoreException, IOException {

  }
}
