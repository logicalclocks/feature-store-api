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
from typing import Dict, Any, Union
import great_expectations as ge

from python.hsfs.expectation_suite import ExpectationSuite


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
            ge.core.ExpectationSuite, ExpectationSuite, None
        ] = None,
        save_report: bool = False,
        validation_options: Dict[str, Any] = {},
        ge_type: bool = True,
    ) -> Union[
        ge.core.ExpectationSuiteValidationResult, validation_report.ValidationReport
    ]:

        if expectation_suite:
            if isinstance(expectation_suite, ExpectationSuite):
                suite = expectation_suite
            else:
                suite = ExpectationSuite.from_ge_type(expectation_suite)
        else:
            suite = feature_group.get_expectation_suite(False)

        if suite is not None:
            run_validation = validation_options.get(
                "run_validation", suite.run_validation
            )
            if run_validation:
                report = engine.get_instance().validate_with_great_expectations(
                    dataframe=dataframe,
                    expectation_suite=suite.to_ge_type(),
                    ge_validate_kwargs=validation_options.get("ge_validate_kwargs", {}),
                )

                save_report = validation_options.get("save_report", save_report)
                if save_report:
                    return feature_group.save_validation_report(report, ge_type=ge_type)

                if ge_type:
                    return validation_report.ValidationReport(
                        **report.to_json_dict()
                    ).to_ge_type()
                else:
                    return validation_report.ValidationReport(**report.to_json_dict())
        return
