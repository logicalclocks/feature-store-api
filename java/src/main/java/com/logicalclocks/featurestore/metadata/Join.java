package com.logicalclocks.featurestore.metadata;

import com.logicalclocks.featurestore.Feature;
import com.logicalclocks.featurestore.JoinType;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class Join {

  @Getter @Setter
  private Query query;

  @Getter @Setter
  private List<Feature> on;
  @Getter @Setter
  private List<Feature> leftOn;
  @Getter @Setter
  private List<Feature> rightOn;

  @Getter @Setter
  private JoinType joinType;

  public Join(Query query, JoinType joinType) {
    this.query = query;
    this.joinType = joinType;
  }

  public Join(Query query, List<Feature> on, JoinType joinType) {
    this.query = query;
    this.on = on;
    this.joinType = joinType;
  }

  public Join(Query query, List<Feature> leftOn, List<Feature> rightOn, JoinType joinType) {
    this.query = query;
    this.leftOn = leftOn;
    this.rightOn = rightOn;
    this.joinType = joinType;
  }
}
