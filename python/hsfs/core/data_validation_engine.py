#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import time

from hsfs import (
    engine,
    expectation,
    rule,
    expectation_result,
    feature_group_validation,
    validation_result,
)
from hsfs.client import exceptions
from hsfs.core import validations_api, expectations_api


class DataValidationEngine:
    def __init__(self, feature_store_id, entity_type):
        self._feature_group_validation_api = validations_api.FeatureGroupValidationsApi(
            feature_store_id, entity_type
        )
        self._expectations_api = expectations_api.ExpectationsApi(
            feature_store_id, entity_type
        )

    def validate(self, feature_group, feature_dataframe):
        """Perform data validation for a dataframe and send the result json to Hopsworks."""
        validation_time = int(round(time.time() * 1000))
        if len(feature_dataframe.head(1)) == 0:
            raise exceptions.FeatureStoreException(
                "There is no data in the entity that you are trying to validate data "
                "for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group."
            )

        expectations = self._expectations_api.get(feature_group=feature_group)

        expectation_results_java = engine.get_instance().validate(
            feature_dataframe, expectations
        )
        # Loop through Java object and convert to Python
        expectation_results = []
        for exp_res in expectation_results_java:
            # Create the Expectation
            exp = exp_res.getExpectation()
            rules_python = []
            for exp_rule in exp.getRules():
                rules_python.append(
                    rule.Rule(
                        name=exp_rule.getName().name(),
                        level=exp_rule.getLevel().name(),
                        min=exp_rule.getMin(),
                        max=exp_rule.getMax(),
                        pattern=exp_rule.getPattern(),
                        accepted_type=exp_rule.getAcceptedType(),
                        legal_values=exp_rule.getLegalValues(),
                    )
                )

            features_python = []
            for feature in exp.getFeatures():
                features_python.append(feature)
            expectation_python = expectation.Expectation(
                name=exp.getName(),
                description=exp.getDescription(),
                features=features_python,
                rules=rules_python,
            )
            # Create the ValidationResult
            validation_results_python = []
            for validation_result_java in exp_res.getResults():
                # Create rule python
                validation_rule_python = rule.Rule(
                    name=validation_result_java.getRule().getName().name(),
                    level=validation_result_java.getRule().getLevel().name(),
                    min=validation_result_java.getRule().getMin(),
                    max=validation_result_java.getRule().getMax(),
                    pattern=validation_result_java.getRule().getPattern(),
                    accepted_type=validation_result_java.getRule().getAcceptedType(),
                    legal_values=validation_result_java.getRule().getLegalValues(),
                )

                validation_results_python.append(
                    validation_result.ValidationResult(
                        status=validation_result_java.getStatus().name(),
                        message=validation_result_java.getMessage(),
                        value=validation_result_java.getValue(),
                        feature=validation_result_java.getFeature(),
                        rule=validation_rule_python,
                    )
                )

            expectation_result_python = expectation_result.ExpectationResult(
                expectation=expectation_python, results=validation_results_python
            )
            expectation_results.append(expectation_result_python)
        validation_python = feature_group_validation.FeatureGroupValidation(
            validation_time=validation_time,
            expectation_results=expectation_results,
        )
        return self._feature_group_validation_api.put(feature_group, validation_python)

    def get_validations(self, feature_group, validation_time=None, commit_time=None):
        """Get feature group data validation results for the specified validation or commit time."""
        return self._feature_group_validation_api.get(
            feature_group, validation_time, commit_time
        )
