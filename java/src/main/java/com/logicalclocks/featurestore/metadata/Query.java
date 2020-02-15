package com.logicalclocks.featurestore.metadata;

import com.logicalclocks.featurestore.*;
import com.logicalclocks.featurestore.engine.SparkEngine;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Query {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);

  @Getter @Setter
  private FeatureGroup leftFeatureGroup;
  @Getter @Setter
  private List<Feature> leftFeatures;

  @Getter @Setter
  private List<Join> joins = new ArrayList<>();

  private QueryConstructorApi queryConstructorApi;

  public Query(FeatureGroup leftFeatureGroup, List<Feature> leftFeatures) throws FeatureStoreException {
    this.leftFeatureGroup = leftFeatureGroup;
    this.leftFeatures = leftFeatures;

    this.queryConstructorApi = new QueryConstructorApi();
  }

  public Query join(Query subquery) {
    return join(subquery, JoinType.INNER);
  }

  public Query join(Query subquery, List<String> on) {
    return joinFeatures(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public Query joinFeatures(Query subquery, List<Feature> on) {
    return joinFeatures(subquery, on, JoinType.INNER);
  }

  public Query join(Query subquery, List<String> leftOn, List<String> rightOn) {
    return joinFeatures(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), JoinType.INNER);
  }

  public Query joinFeatures(Query subquery, List<Feature> leftOn, List<Feature> rightOn) {
    return joinFeatures(subquery, leftOn, rightOn, JoinType.INNER);
  }

  public Query join(Query subquery, JoinType joinType) {
    joins.add(new Join(subquery, joinType));
    return this;
  }

  public Query join(Query subquery, List<String> on, JoinType joinType) {
    joins.add(new Join(subquery, on.stream().map(Feature::new).collect(Collectors.toList()), joinType));
    return this;
  }

  public Query joinFeatures(Query subquery, List<Feature> on, JoinType joinType) {
    joins.add(new Join(subquery, on, joinType));
    return this;
  }

  public Query join(Query subquery, List<String> leftOn, List<String> rightOn, JoinType joinType) {
    joins.add(new Join(subquery, leftOn.stream().map(Feature::new).collect(Collectors.toList()),
        rightOn.stream().map(Feature::new).collect(Collectors.toList()), joinType));
    return this;
  }

  public Query joinFeatures(Query subquery, List<Feature> leftOn, List<Feature> rightOn, JoinType joinType) {
    joins.add(new Join(subquery, leftOn, rightOn, joinType));
    return this;
  }

  public Dataset<Row> read() throws FeatureStoreException, IOException {
    String sqlQuery =
        queryConstructorApi.constructQuery(leftFeatureGroup.getFeatureStore(), this);
    LOGGER.info("Executing query: " + sqlQuery);
    return SparkEngine.getInstance().sql(sqlQuery);
  }

  public void show(int numRows) throws FeatureStoreException, IOException {
    String sqlQuery =
        queryConstructorApi.constructQuery(leftFeatureGroup.getFeatureStore(), this);
    LOGGER.info("Executing query: " + sqlQuery);
    SparkEngine.getInstance().sql(sqlQuery).show(numRows);
  }
}
