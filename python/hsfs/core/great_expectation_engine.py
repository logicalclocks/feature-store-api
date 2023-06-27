#
#   Copyright 2022 Hopsworks AB
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

from hsfs import engine, validation_report
from hsfs import expectation_suite as es
from typing import Dict, Any, Optional, Union
import great_expectations as ge


class GreatExpectationEngine:
    def __init__(self, feature_store_id: int):
        """Engine to run validation using Great Expectations.

        :param feature_store_id: id of the respective Feature Store
        :type `int`
        :return: The engine for Great Expectation objects
        :rtype: `GreatExpectationEngine`
        """
        self._feature_store_id = feature_store_id

    def validate(
        self,
        feature_group,
        dataframe,
        expectation_suite: Union[
            ge.core.ExpectationSuite, es.ExpectationSuite, None
        ] = None,
        save_report: bool = False,
        validation_options: Dict[str, Any] = {},
        ge_type: bool = True,
        ingestion_result: str = "UNKNOWN",
    ) -> Union[
        ge.core.ExpectationSuiteValidationResult,
        validation_report.ValidationReport,
        None,
    ]:
        suite = self.fetch_or_convert_expectation_suite(
            feature_group, expectation_suite, validation_options
        )

        if self.should_run_validation(
            expectation_suite=suite, validation_options=validation_options
        ):
            report = engine.get_instance().validate_with_great_expectations(
                dataframe=dataframe,
                expectation_suite=suite.to_ge_type(),
                ge_validate_kwargs=validation_options.get("ge_validate_kwargs", {}),
            )
        else:
            # if run_validation is False we skip validation and saving_report
            return

        if report.success:
            print("Validation succeeded.")
        else:
            print("Validation failed.")
            if (
                suite.validation_ingestion_policy == "STRICT"
                and ingestion_result == "INGESTED"
            ):
                ingestion_result = "REJECTED"

        return self.save_or_convert_report(
            feature_group=feature_group,
            report=report,
            save_report=save_report,
            validation_options=validation_options,
            ingestion_result=ingestion_result,
            ge_type=ge_type,
        )

    def fetch_or_convert_expectation_suite(
        self,
        feature_group,
        expectation_suite: Union[
            ge.core.ExpectationSuite, es.ExpectationSuite, None
        ] = None,
        validation_options: dict = {},
    ) -> Optional[es.ExpectationSuite]:
        """Convert provided expectation suite or fetch the one attached to the Feature Group from backend."""
        if expectation_suite is not None:
            if isinstance(expectation_suite, es.ExpectationSuite):
                return expectation_suite
            return es.ExpectationSuite.from_ge_type(expectation_suite)
        if validation_options.get("fetch_expectation_suite", True):
            return feature_group.get_expectation_suite(False)
        return feature_group.expectation_suite

    def should_run_validation(
        self,
        expectation_suite: Optional[es.ExpectationSuite],
        validation_options: Dict[str, Any],
    ) -> bool:
        # Suite is None if not provided and nothing attached to FG.
        # In that case we skip validation
        if expectation_suite is None:
            return False

        # If "run_validation" is provided it overrides the value of run_validation of the suite
        return validation_options.get(
            "run_validation", expectation_suite.run_validation
        )

    def save_or_convert_report(
        self,
        feature_group,
        report: ge.core.ExpectationSuiteValidationResult,
        save_report: bool,
        ge_type: bool,
        validation_options: Dict[str, Any],
        ingestion_result: str = "UNKNOWN",
    ) -> Union[
        ge.core.ExpectationSuiteValidationResult, validation_report.ValidationReport
    ]:
        save_report = validation_options.get("save_report", save_report)
        if save_report:
            return feature_group.save_validation_report(
                report, ingestion_result=ingestion_result, ge_type=ge_type
            )

        if ge_type:
            return report
        else:
            return validation_report.ValidationReport(
                **report.to_json_dict(), ingestion_result=ingestion_result
            )
