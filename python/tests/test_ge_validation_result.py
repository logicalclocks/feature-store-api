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


from hsfs import ge_validation_result


class TestValidationResult:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get"]["response"]

        # Act
        vr = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert vr.id == 11
        assert vr.success is True
        assert vr._observed_value == "test_observed_value"
        assert vr._expectation_id == 22
        assert vr._validation_report_id == 33
        assert vr.result == {"result_key": "result_value"}
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.exception_info == {"exception_info_key": "exception_info_value"}
        assert vr.expectation_config == {
            "expectation_config_key": "expectation_config_value"
        }

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get_basic_info"]["response"]

        # Act
        vr = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert vr.id is None
        assert vr.success is True
        assert vr._observed_value is None
        assert vr._expectation_id is None
        assert vr._validation_report_id is None
        assert vr.result == {"result_key": "result_value"}
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.exception_info == {"exception_info_key": "exception_info_value"}
        assert vr.expectation_config == {
            "expectation_config_key": "expectation_config_value"
        }

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get_list"]["response"]

        # Act
        vr_list = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert len(vr_list) == 1
        vr = vr_list[0]
        assert vr.id == 11
        assert vr.success is True
        assert vr._observed_value == "test_observed_value"
        assert vr._expectation_id == 22
        assert vr._validation_report_id == 33
        assert vr.result == {"result_key": "result_value"}
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.exception_info == {"exception_info_key": "exception_info_value"}
        assert vr.expectation_config == {
            "expectation_config_key": "expectation_config_value"
        }

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get_list_empty"]["response"]

        # Act
        vr_list = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert len(vr_list) == 0

    def test_timezone_support_validation_time(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get"]["response"]
        json["validation_time"] = "2023-02-15T09:20:03.000414-05"

        # Act
        vr = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert vr.id == 11
        assert vr.validation_time == 1676470803000
