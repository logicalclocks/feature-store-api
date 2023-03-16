#
#   Copyright 2023 Hopsworks AB
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

from hsfs.feature_monitoring_result import FeatureMonitoringResult


class TestFeatureMonitoringResult:
    def test_from_response_json_via_fg(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"][
            "get_via_feature_group"
        ]["response"]

        # Act
        result = FeatureMonitoringResult.from_response_json(result_json)

        # Assert
        assert isinstance(result, FeatureMonitoringResult)
        assert result._id == 42
        assert result._execution_id == 123
        assert result._detection_stats_id == 333
        assert result._reference_stats_id == 222
        assert result._config_id == 32
        assert result._monitoring_time == 1676457000000
        assert result._difference == 0.3
        assert result._shift_detected is True
        assert result._href[-2:] == "32"

    def test_from_response_json_via_fv(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"][
            "get_via_feature_view"
        ]["response"]

        # Act
        result = FeatureMonitoringResult.from_response_json(result_json)

        # Assert
        assert isinstance(result, FeatureMonitoringResult)
        assert result._id == 42
        assert result._execution_id == 123
        assert result._detection_stats_id == 333
        assert result._reference_stats_id == 222
        assert result._config_id == 32
        assert result._monitoring_time == 1676457000000
        assert result._difference == 0.3
        assert result._shift_detected is True
        assert result._href[-2:] == "32"

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"]["get_list"][
            "response"
        ]

        # Act
        result_list = FeatureMonitoringResult.from_response_json(result_json)
        result = result_list[0]

        # Assert
        assert isinstance(result_list, list)
        assert len(result_list) == 1
        assert isinstance(result, FeatureMonitoringResult)
        assert result._id == 42
        assert result._execution_id == 123
        assert result._detection_stats_id == 333
        assert result._reference_stats_id == 222
        assert result._config_id == 32
        assert result._monitoring_time == 1676457000000
        assert result._difference == 0.3
        assert result._shift_detected is True
        assert result._href[-2:] == "32"

    def test_from_response_json_empty_list(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"]["get_list_empty"][
            "response"
        ]

        # Act
        result_list = FeatureMonitoringResult.from_response_json(result_json)

        # Assert
        assert isinstance(result_list, list)
        assert len(result_list) == 0
