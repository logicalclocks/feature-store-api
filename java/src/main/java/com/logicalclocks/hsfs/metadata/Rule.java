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

package com.logicalclocks.hsfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Arrays;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Rule extends RestDto<Rule> {

  @Getter @Setter
  private Name name;
  @Getter @Setter
  private Predicate predicate;
  @Getter @Setter
  private ValueType valueType;
  @Getter @Setter
  private String description;
  @Getter @Setter
  private String feature;
  private RuleConfiguration ruleConfiguration;

  public ConstraintGroupLevel getLevel() {
    return ruleConfiguration != null ? ruleConfiguration.getLevel() : null;
  }

  public void setLevel(ConstraintGroupLevel level) {
    if (ruleConfiguration == null) {
      ruleConfiguration = RuleConfiguration.builder().level(level).build();
    } else {
      ruleConfiguration.setLevel(level);
    }
  }

  public Double getMax() {
    return ruleConfiguration != null ? ruleConfiguration.getMax() : null;
  }

  public void setMax(Double max) {
    if (ruleConfiguration == null) {
      ruleConfiguration = RuleConfiguration.builder().max(max).build();
    } else {
      ruleConfiguration.setMax(max);
    }
  }

  public Double getMin() {
    return ruleConfiguration != null ? ruleConfiguration.getMin() : null;
  }

  public void setMin(Double min) {
    if (ruleConfiguration == null) {
      ruleConfiguration = RuleConfiguration.builder().min(min).build();
    } else {
      ruleConfiguration.setMin(min);
    }
  }

  public String getPattern() {
    return ruleConfiguration != null ? ruleConfiguration.getPattern() : null;
  }

  public void setPattern(String pattern) {
    if (ruleConfiguration == null) {
      ruleConfiguration = RuleConfiguration.builder().pattern(pattern).build();
    } else {
      ruleConfiguration.setPattern(pattern);
    }
  }

  public String[] getLegalValues() {
    return ruleConfiguration != null ? ruleConfiguration.getLegalValues() : null;
  }

  public void setLegalValues(String[] legalValues) {
    if (ruleConfiguration == null) {
      ruleConfiguration = RuleConfiguration.builder().legalValues(legalValues).build();
    } else {
      ruleConfiguration.setLegalValues(legalValues);
    }
  }

  protected RuleConfiguration getRuleConfiguration() {
    return ruleConfiguration;
  }

  public enum ConstraintGroupLevel {
    Warning,
    Error;
  }

  public enum Predicate {
    VALUE,
    LEGAL_VALUES,
    ACCEPTED_TYPE,
    PATTERN
  }

  public enum Name {
    HAS_MEAN,
    HAS_MIN,
    HAS_MAX,
    HAS_SUM
  }

  public enum ValueType {
    INTEGER, DOUBLE, STRING, STRING_ARRAY
  }

  @Override
  public String toString() {
    return "Rule{"
      + "name=" + name
      + ", predicate=" + predicate
      + ", valueType=" + valueType
      + ", description='" + description + '\''
      + ", feature='" + feature + '\''
      + ", level=" + getLevel()
      + ", min=" + getMin()
      + ", max=" + getMax()
      + ", pattern=" + getPattern()
      + ", legalValues=" + Arrays.toString(getLegalValues())
      + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Rule rule = (Rule) o;
    return name == rule.name
      && Objects.equals(feature, rule.feature);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, feature);
  }
}
