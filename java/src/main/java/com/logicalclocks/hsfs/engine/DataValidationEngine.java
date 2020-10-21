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

package com.logicalclocks.hsfs.engine;

import com.amazon.deequ.checks.Check;
import com.amazon.deequ.checks.CheckResult;
import com.amazon.deequ.constraints.ConstraintResult;
import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.DataValidationResult;
import com.logicalclocks.hsfs.metadata.FeatureGroupValidation;
import com.logicalclocks.hsfs.metadata.FeatureGroupValidationsApi;
import com.logicalclocks.hsfs.metadata.Rule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataValidationEngine implements DataValidationIntf {

  private final FeatureGroupValidationsApi featureGroupValidationsApi;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataValidationEngine.class);

  public DataValidationEngine(EntityEndpointType entityType) {
    this.featureGroupValidationsApi = new FeatureGroupValidationsApi(entityType);
  }

  @Override
  public FeatureGroupValidation runVerification(Dataset<Row> data, List<Rule> rules) {
    List<ConstraintGroup> constraintGroups = new ArrayList<>();
    Map<Rule.ConstraintGroupLevel, List<Constraint>> constraintGroupLevels = new HashMap<>();
    // Check if there is another rule with same name and same feature and same min or max
    for (Rule rule : rules) {
      // If constraint with same name and predicate exists, then
      Constraint constraint =
          new Constraint(rule.getName().name(), Option.apply(rule.getName().name()),
          Option.apply(
            JavaConverters.asScalaIteratorConverter(Collections.singletonList(rule.getFeature()).iterator()).asScala()
              .toSeq()),
          Option.apply(rule.getMin()),
          Option.apply(rule.getMax()),
          Option.apply(null),
          Option.apply(rule.getPattern()),
          Option.apply(rule.getValueType().name()),
          Option.apply(rule.getLegalValues()));

      if (!constraintGroupLevels.containsKey(rule.getLevel())) {
        constraintGroupLevels.put(rule.getLevel(), new ArrayList<>());
      }
      constraintGroupLevels.get(rule.getLevel()).add(constraint);
    }
    if (!constraintGroupLevels.isEmpty()) {
      for (Rule.ConstraintGroupLevel level : constraintGroupLevels.keySet()) {
        ConstraintGroup constraintGroup = new ConstraintGroup(level.name(), level.name(),
            JavaConverters.asScalaIteratorConverter(constraintGroupLevels.get(level).iterator()).asScala().toSeq());
        constraintGroups.add(constraintGroup);
      }
    }

    String validationTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    Map<Check, CheckResult> deequResults = DeequEngine.runVerificationDeequ(data,
        JavaConverters.asScalaIteratorConverter(constraintGroups.iterator()).asScala().toSeq());

    List<DataValidationResult> dataValidationResults = new ArrayList<>();

    for (Check check : deequResults.keySet()) {
      List<ConstraintResult> constraintResultsList =
          DeequEngine.getConstraintResults(deequResults.get(check).constraintResults());
      for (ConstraintResult constraintResult : constraintResultsList) {
        String[] constraintInfo = constraintResult.constraint().toString().split("\\W+");
        Rule.Name ruleName = getRuleNameFromDeequ(constraintInfo[1]);
        // Find rule from list of rules that Deequ used for validation
        Rule rule =
            rules.stream().filter(x -> x.getName() == ruleName && x.getFeature().equals(constraintInfo[2]))
            .findFirst().get();

        dataValidationResults.add(DataValidationResult.builder()
            .status(DataValidationResult.Status.fromDeequStatus(constraintResult.status()))
            .rule(rule)
            .message(!constraintResult.message().isEmpty() ?  constraintResult.message().get() : "Success")
            .value(String.valueOf(constraintResult.metric().get().value().get()))
            .build());
      }
    }
    return FeatureGroupValidation.builder().validationTime(validationTime).validations(dataValidationResults).build();
  }

  public FeatureGroupValidation sendResults(FeatureGroup featureGroup, FeatureGroupValidation results)
      throws FeatureStoreException, IOException {
    return  featureGroupValidationsApi.put(featureGroup, results);
  }

  public List<FeatureGroupValidation> getValidations(FeatureGroup featureGroup)
      throws FeatureStoreException, IOException {
    return featureGroupValidationsApi.get(featureGroup);
  }

  public FeatureGroupValidation getValidation(FeatureGroup featureGroup, String validationTime)
      throws FeatureStoreException, IOException {
    return featureGroupValidationsApi.get(featureGroup, validationTime);
  }

  private Rule.Name getRuleNameFromDeequ(String deequName) {
    switch (deequName) {
      case "Maximum":
        return Rule.Name.HAS_MAX;
      case "Minimum":
        return Rule.Name.HAS_MIN;
      case "Mean":
        return Rule.Name.HAS_MEAN;
      default:
        throw new UnsupportedOperationException("Deequ rule not supported");
    }
  }

  public enum Engine {
    DEEQU("deequ");

    private final String name;

    Engine(String name) {
      this.name = name;
    }

    public static Engine fromString(String name) {
      return valueOf(name.toUpperCase());
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

}
