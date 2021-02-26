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
import com.logicalclocks.hsfs.metadata.Expectation;
import com.logicalclocks.hsfs.metadata.ExpectationResult;
import com.logicalclocks.hsfs.metadata.FeatureGroupValidation;
import com.logicalclocks.hsfs.metadata.FeatureGroupValidationsApi;
import com.logicalclocks.hsfs.metadata.ValidationResult;
import com.logicalclocks.hsfs.metadata.validation.Level;
import com.logicalclocks.hsfs.metadata.validation.RuleName;
import com.logicalclocks.hsfs.metadata.validation.Rule;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataValidationEngine {

  private static DataValidationEngine INSTANCE = null;

  public static synchronized DataValidationEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new DataValidationEngine();
    }
    return INSTANCE;
  }

  private final FeatureGroupValidationsApi featureGroupValidationsApi =
      new FeatureGroupValidationsApi(EntityEndpointType.FEATURE_GROUP);

  public FeatureGroupValidation validate(Dataset<Row> data, FeatureGroup featureGroup, List<Expectation> expectations)
      throws FeatureStoreException, IOException {
    List<ExpectationResult> expectationResults = validate(data, expectations);
    return featureGroupValidationsApi.put(featureGroup,
      FeatureGroupValidation.builder()
        .validationTime(Instant.now().toEpochMilli())
        .expectationResults(expectationResults).build());
  }

  public List<ExpectationResult> validate(Dataset<Row> data, List<Expectation> expectations) {
    // Loop through all feature group expectations, then loop all features and rules of the expectation and
    // create constraints for Deequ.
    List<ExpectationResult> expectationResults = new ArrayList<>();
    for (Expectation expectation : expectations) {
      List<ConstraintGroup> constraintGroups = new ArrayList<>();
      Map<Level, List<Constraint>> constraintGroupLevels = new HashMap<>();
      List<ValidationResult> validationResults = new ArrayList<>();
      for (String feature : expectation.getFeatures()) {
        for (Rule rule : expectation.getRules()) {
          // If constraint with same name and predicate exists, then
          Constraint constraint =
              new Constraint(rule.getName().name(),
              Option.apply(rule.getName().name()),
              Option.apply(JavaConverters.asScalaBufferConverter(Collections.singletonList(feature)).asScala().toSeq()),
              Option.apply(rule.getMin()),
              Option.apply(rule.getMax()),
              Option.apply(null),
              Option.apply(rule.getPattern()),
              Option.apply(rule.getAcceptedType()),
              Option.apply(rule.getLegalValues()));

          if (!constraintGroupLevels.containsKey(rule.getLevel())) {
            constraintGroupLevels.put(rule.getLevel(), new ArrayList<>());
          }
          constraintGroupLevels.get(rule.getLevel()).add(constraint);
        }
      }
      if (!constraintGroupLevels.isEmpty()) {
        for (Level level : constraintGroupLevels.keySet()) {
          ConstraintGroup constraintGroup = new ConstraintGroup(level.name(), level.name(),
              JavaConverters.asScalaIteratorConverter(constraintGroupLevels.get(level).iterator()).asScala().toSeq());
          constraintGroups.add(constraintGroup);
        }
      }

      // Run Deequ verification suite and return results
      Map<Check, CheckResult> deequResults = DeequEngine.runVerification(data,
          JavaConverters.asScalaIteratorConverter(constraintGroups.iterator()).asScala().toSeq());

      // Parse Deequ results and convert to Feature Group validation results
      for (Check check : deequResults.keySet()) {
        List<ConstraintResult> constraintResultsList =
            DeequEngine.getConstraintResults(deequResults.get(check).constraintResults());
        for (ConstraintResult constraintResult : constraintResultsList) {
          String[] constraintInfo = constraintResult.constraint().toString().split("\\W+");
          RuleName ruleName = getRuleNameFromDeequ(constraintInfo[1]);
          // Find rule from list of rules that Deequ used for validation
          for (String feature : expectation.getFeatures()) {
            for (Rule rule : expectation.getRules()) {
              if (rule.getName() == ruleName && feature.equals(constraintInfo[2])) {
                validationResults.add(ValidationResult.builder()
                    .status(ExpectationResult.Status.fromDeequStatus(constraintResult.status()))
                    .feature(feature)
                    .rule(rule)
                    .message(!constraintResult.message().isEmpty() ? constraintResult.message().get() : "Success")
                    .value(String.valueOf(constraintResult.metric().get().value().get()))
                    .build());
                break;
              }
            }
          }
        }
      }
      expectationResults.add(ExpectationResult.builder().expectation(expectation).results(validationResults).build());
    }
    return expectationResults;
  }

  public List<FeatureGroupValidation> getValidations(FeatureGroup featureGroup)
      throws FeatureStoreException, IOException {
    return featureGroupValidationsApi.get(featureGroup);
  }

  public FeatureGroupValidation getValidation(FeatureGroup featureGroup, ImmutablePair<ValidationTimeType, Long> pair)
      throws FeatureStoreException, IOException {
    return featureGroupValidationsApi.get(featureGroup, pair);
  }

  public RuleName getRuleNameFromDeequ(String deequName) {
    switch (deequName) {
      case "Maximum":
        return RuleName.HAS_MAX;
      case "Minimum":
        return RuleName.HAS_MIN;
      case "Mean":
        return RuleName.HAS_MEAN;
      case "Size":
        return RuleName.HAS_SIZE;
      case "Completeness":
        return RuleName.HAS_COMPLETENESS;
      case "Uniqueness":
        return RuleName.HAS_UNIQUENESS;
      case "hasDistinctness":
        return RuleName.HAS_DISTINCTNESS;
      case "hasUniqueValueRatio":
        return RuleName.HAS_UNIQUE_VALUE_RATIO;

      default:
        throw new UnsupportedOperationException("Deequ rule not supported");
    }
  }

  public enum ValidationTimeType {
    VALIDATION_TIME,
    COMMIT_TIME
  }

}
