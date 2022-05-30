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


class GreatExpectationEngine:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def validate(self, feature_group, dataframe, save_report, validation_options):

        suite = feature_group.get_expectation_suite(False)
        if suite is not None:
            run_validation = validation_options.get(
                "run_validation", suite.run_validation
            )
            if (
                run_validation
                and engine.get_type() == "python"
                and feature_group.stream
            ) or (
                run_validation
                and engine.get_type() == "spark"
                and not feature_group.stream
            ):
                report = engine.get_instance().validate_with_great_expectations(
                    dataframe=dataframe,
                    expectation_suite=suite.to_ge_type(),
                    ge_validate_kwargs=validation_options.get("ge_validate_kwargs", {}),
                )

                save_report = validation_options.get("save_report", save_report)
                if save_report:
                    return feature_group.save_validation_report(report, ge_type=False)

                return validation_report.ValidationReport(**report.to_json_dict())
        return
