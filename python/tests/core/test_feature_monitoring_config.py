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
from hsfs.core.feature_monitoring_config import FeatureMonitoringConfig


class TestFeatureMonitoringConfig:
    def test_from_response_json_via_fg(self, backend_fixtures):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["detection_insert_reference_snapshot"]["response"]

        # Act
        config = FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        assert config._id == 32
        assert config._feature_store_id == 67
        assert config._feature_group_id == 13
        assert config._href[-2:] == "32"
        assert config._feature_name == "monitored_feature"
        assert config._enabled is True
        assert config._name == "unit_test_config"
        assert config._feature_monitoring_type == "DESCRIPTIVE_STATISTICS"
        assert isinstance(config._alert_config, str)
        assert isinstance(config._scheduler_config, str)

        assert config._detection_window_config.window_config_type == "INSERT"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._reference_window_config.window_config_type == "SNAPSHOT"
        assert config._reference_window_config.time_offset == "LAST"

        assert config._statistics_comparison_config["threshold"] == 1
        assert config._statistics_comparison_config["strict"] is True
        assert config._statistics_comparison_config["relative"] is False
        assert config._statistics_comparison_config["compare_on"] == "MEAN"

    def test_from_response_json_via_fv(self, backend_fixtures):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["detection_insert_reference_snapshot"]["response"]

        # Act
        config = FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        assert config._id == 32
        assert config._feature_store_id == 67
        assert config._feature_view_id == 22
        assert config._feature_group_id is None
        assert config._href[-2:] == "32"
        assert config._feature_name == "monitored_feature"
        assert config._enabled is True
        assert config._name == "unit_test_config"
        assert config._feature_monitoring_type == "DESCRIPTIVE_STATISTICS"
        assert isinstance(config._alert_config, str)
        assert isinstance(config._scheduler_config, str)

        assert config._detection_window_config.window_config_type == "BATCH"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._reference_window_config.window_config_type == "TRAINING_DATASET"
        assert config._reference_window_config.specific_id == 33

        assert config._statistics_comparison_config["threshold"] == 1
        assert config._statistics_comparison_config["strict"] is True
        assert config._statistics_comparison_config["relative"] is False
        assert config._statistics_comparison_config["compare_on"] == "MEAN"

    def test_from_response_json_stats_only_via_fg(self, backend_fixtures):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["detection_insert_scheduled_stats_only"]["response"]

        # Act
        config = FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        assert config._id == 32
        assert config._feature_store_id == 67
        assert config._feature_view_id is None
        assert config._feature_group_id == 13
        assert config._href[-2:] == "32"
        assert config._name == "unit_test_config"
        assert config._feature_name == "monitored_feature"
        assert config._enabled is True
        assert config._feature_monitoring_type == "SCHEDULED_STATISTICS"
        assert isinstance(config._scheduler_config, str)

        assert config._detection_window_config.window_config_type == "INSERT"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"

    def test_from_response_json_stats_only_via_fv(self, backend_fixtures):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["detection_insert_scheduled_stats_only"]["response"]

        # Act
        config = FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        assert config._id == 32
        assert config._feature_store_id == 67
        assert config._feature_view_id == 22
        assert config._feature_group_id is None
        assert config._href[-2:] == "32"
        assert config._name == "unit_test_config"
        assert config._feature_name == "monitored_feature"
        assert config._enabled is True
        assert config._feature_monitoring_type == "SCHEDULED_STATISTICS"
        assert isinstance(config._scheduler_config, str)

        assert config._detection_window_config.window_config_type == "BATCH"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"]["get_list"][
            "response"
        ]

        # Act
        config_list = FeatureMonitoringConfig.from_response_json(config_json)
        config = config_list[0]

        # Assert
        assert isinstance(config_list, list)
        assert len(config_list) == 1
        assert isinstance(config, FeatureMonitoringConfig)
        assert config._id == 32
        assert config._feature_store_id == 67
        assert config._feature_view_id == 22
        assert config._feature_group_id is None
        assert config._href[-2:] == "32"
        assert config._feature_name == "monitored_feature"
        assert config._enabled is True
        assert config._feature_monitoring_type == "DESCRIPTIVE_STATISTICS"
        assert isinstance(config._alert_config, str)
        assert isinstance(config._scheduler_config, str)

        assert config._detection_window_config.window_config_type == "BATCH"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._reference_window_config.window_config_type == "TRAINING_DATASET"
        assert config._reference_window_config.specific_id == 33

        assert config._statistics_comparison_config["threshold"] == 1
        assert config._statistics_comparison_config["strict"] is True
        assert config._statistics_comparison_config["relative"] is False
        assert config._statistics_comparison_config["compare_on"] == "MEAN"

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"]["get_list_empty"][
            "response"
        ]

        # Act
        config_list = FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        assert isinstance(config_list, list)
        assert len(config_list) == 0
