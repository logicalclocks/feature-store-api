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

from hsfs.core import feature_monitoring_config_engine
from hsfs.core.job_scheduler import JobScheduler

DEFAULT_DESCRIPTION = "A feature monitoring configuration for unit test."
DEFAULT_NAME = "test_monitoring_config"
DEFAULT_FEATURE_NAME = "monitored_feature"
DEFAULT_FEATURE_MONITORING_CONFIG_CREATE_API = (
    "hsfs.core.feature_monitoring_config_api.FeatureMonitoringConfigApi.create"
)
DEFAULT_FEATURE_MONITORING_CONFIG_SETUP_JOB_API = "hsfs.core.feature_monitoring_config_api.FeatureMonitoringConfigApi.setup_feature_monitoring_job"
DEFAULT_FEATURE_STORE_ID = 67
DEFAULT_FEATURE_GROUP_ID = 13
DEFAULT_FEATURE_VIEW_ID = 22
DEFAULT_FEATURE_VIEW_NAME = "feature_view_unittest"
DEFAULT_FEATURE_VIEW_VERSION = 2
DEFAULT_ALERT_CONFIG = "alert_config"
DEFAULT_SCHEDULER_CONFIG = {
    "job_frequency": "HOURLY",
    "start_date_time": 1676457000,
    "enabled": True,
}


class TestFeatureMonitoringConfigEngine:
    # This needs more extensive unit testing once we have implemented some logic to
    # verify the compatibility of the different args
    def test_build_monitoring_window_config(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        window_config = config_engine.build_monitoring_window_config(
            window_config_type="SNAPSHOT",
            time_offset="LAST",
            row_percentage=10,
        )

        # Assert
        assert window_config.window_config_type == "SNAPSHOT"
        assert window_config.time_offset == "LAST"
        assert window_config.row_percentage == 10

    def test_build_feature_monitoring_config(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        detection_window_config = config_engine.build_monitoring_window_config(
            window_config_type="INSERT",
            time_offset="1w",
            window_length="1d",
        )
        reference_window_config = config_engine.build_monitoring_window_config(
            window_config_type="SPECIFIC_VALUE", specific_value=2
        )

        stats_comparison_configuration = {
            "threshold": 1,
            "strict": True,
            "relative": False,
            "metric": "MEAN",
        }

        # Act
        config = config_engine._build_feature_monitoring_config(
            name=DEFAULT_NAME,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_window_config=detection_window_config,
            reference_window_config=reference_window_config,
            statistics_comparison_config=stats_comparison_configuration,
            description=DEFAULT_DESCRIPTION,
            scheduler_config=DEFAULT_SCHEDULER_CONFIG,
            alert_config=DEFAULT_ALERT_CONFIG,
        )

        # Assert
        assert config._feature_store_id == DEFAULT_FEATURE_STORE_ID
        assert config._feature_group_id == DEFAULT_FEATURE_GROUP_ID
        assert config._feature_name == DEFAULT_FEATURE_NAME
        assert config._name == DEFAULT_NAME
        assert config._description == DEFAULT_DESCRIPTION
        assert config._enabled is True
        assert config._feature_monitoring_type == "DESCRIPTIVE_STATISTICS"
        assert config._alert_config == DEFAULT_ALERT_CONFIG
        assert config._detection_window_config.window_config_type == "INSERT"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._reference_window_config.window_config_type == "SPECIFIC_VALUE"
        assert config._reference_window_config.specific_value == 2
        assert (
            config._statistics_comparison_config["threshold"]
            == stats_comparison_configuration["threshold"]
        )
        assert (
            config._statistics_comparison_config["strict"]
            == stats_comparison_configuration["strict"]
        )
        assert (
            config._statistics_comparison_config["relative"]
            == stats_comparison_configuration["relative"]
        )
        assert (
            config._statistics_comparison_config["metric"]
            == stats_comparison_configuration["metric"]
        )

        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    def test_enable_feature_monitoring_config_fg(self, mocker):
        # Arrange
        mock_config_api = mocker.patch(
            "hsfs.core.feature_monitoring_config_api.FeatureMonitoringConfigApi.create"
        )

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        detection_window_config = config_engine.build_monitoring_window_config(
            window_config_type="INSERT",
            time_offset="1w",
            window_length="1d",
        )
        reference_window_config = config_engine.build_monitoring_window_config(
            window_config_type="SNAPSHOT", specific_value=2
        )

        stats_comparison_configuration = {
            "threshold": 1,
            "strict": True,
            "relative": False,
            "metric": "MEAN",
        }

        # Act
        config_engine.enable_feature_monitoring_config(
            name=DEFAULT_NAME,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_window_config=detection_window_config,
            reference_window_config=reference_window_config,
            statistics_comparison_config=stats_comparison_configuration,
            description=DEFAULT_DESCRIPTION,
            scheduler_config=DEFAULT_SCHEDULER_CONFIG,
            alert_config=DEFAULT_ALERT_CONFIG,
        )

        # Assert
        config = mock_config_api.call_args[1]["fm_config"]
        assert config._feature_store_id == DEFAULT_FEATURE_STORE_ID
        assert config._feature_group_id == DEFAULT_FEATURE_GROUP_ID
        assert config._feature_view_id is None
        assert config._feature_name == DEFAULT_FEATURE_NAME
        assert config._name == DEFAULT_NAME
        assert config._description == DEFAULT_DESCRIPTION
        assert config._enabled is True
        assert config._feature_monitoring_type == "DESCRIPTIVE_STATISTICS"
        assert config._alert_config == DEFAULT_ALERT_CONFIG
        assert config._detection_window_config.window_config_type == "INSERT"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._reference_window_config.window_config_type == "SNAPSHOT"
        assert config._reference_window_config.specific_value == 2
        assert (
            config._statistics_comparison_config["threshold"]
            == stats_comparison_configuration["threshold"]
        )
        assert (
            config._statistics_comparison_config["strict"]
            == stats_comparison_configuration["strict"]
        )
        assert (
            config._statistics_comparison_config["relative"]
            == stats_comparison_configuration["relative"]
        )
        assert (
            config._statistics_comparison_config["metric"]
            == stats_comparison_configuration["metric"]
        )
        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    def test_enable_feature_monitoring_config_fv(self, mocker):
        # Arrange
        mock_config_api = mocker.patch(DEFAULT_FEATURE_MONITORING_CONFIG_CREATE_API)

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_id=DEFAULT_FEATURE_VIEW_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )

        detection_window_config = config_engine.build_monitoring_window_config(
            window_config_type="BATCH",
            time_offset="1w",
            window_length="1d",
        )
        reference_window_config = config_engine.build_monitoring_window_config(
            window_config_type="TRAINING_DATASET", specific_id=12
        )

        stats_comparison_configuration = {
            "threshold": 1,
            "strict": True,
            "relative": False,
            "metric": "MEAN",
        }

        # Act
        config_engine.enable_feature_monitoring_config(
            name=DEFAULT_NAME,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_window_config=detection_window_config,
            reference_window_config=reference_window_config,
            statistics_comparison_config=stats_comparison_configuration,
            description=DEFAULT_DESCRIPTION,
            scheduler_config=DEFAULT_SCHEDULER_CONFIG,
            alert_config=DEFAULT_ALERT_CONFIG,
        )

        # Assert
        config = mock_config_api.call_args[1]["fm_config"]
        assert config._feature_store_id == DEFAULT_FEATURE_STORE_ID
        assert config._feature_group_id is None
        assert config._feature_view_id == DEFAULT_FEATURE_VIEW_ID
        assert config._feature_name == DEFAULT_FEATURE_NAME
        assert config._name == DEFAULT_NAME
        assert config._description == DEFAULT_DESCRIPTION
        assert config._enabled is True
        assert config._feature_monitoring_type == "DESCRIPTIVE_STATISTICS"
        assert config._alert_config == DEFAULT_ALERT_CONFIG
        assert config._detection_window_config.window_config_type == "BATCH"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._reference_window_config.window_config_type == "TRAINING_DATASET"
        assert config._reference_window_config.specific_id == 12
        assert (
            config._statistics_comparison_config["threshold"]
            == stats_comparison_configuration["threshold"]
        )
        assert (
            config._statistics_comparison_config["strict"]
            == stats_comparison_configuration["strict"]
        )
        assert (
            config._statistics_comparison_config["relative"]
            == stats_comparison_configuration["relative"]
        )
        assert (
            config._statistics_comparison_config["metric"]
            == stats_comparison_configuration["metric"]
        )
        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    def test_build_stats_monitoring_only_config(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        detection_window_config = config_engine.build_monitoring_window_config(
            window_config_type="INSERT",
            time_offset="1w",
            window_length="1d",
        )

        # Act
        config = config_engine._build_stats_monitoring_only_config(
            name=DEFAULT_NAME,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_window_config=detection_window_config,
            description=DEFAULT_DESCRIPTION,
            scheduler_config=DEFAULT_SCHEDULER_CONFIG,
        )

        # Assert
        assert config._feature_store_id == DEFAULT_FEATURE_STORE_ID
        assert config._feature_group_id == DEFAULT_FEATURE_GROUP_ID
        assert config._feature_name == DEFAULT_FEATURE_NAME
        assert config._enabled is True
        assert config._name == DEFAULT_NAME
        assert config._description == DEFAULT_DESCRIPTION
        assert config._feature_monitoring_type == "SCHEDULED_STATISTICS"
        assert config._detection_window_config.window_config_type == "INSERT"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"

        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    def test_enable_descriptive_statistics_monitoring_fg(self, mocker):
        # Arrange
        mock_config_api = mocker.patch(DEFAULT_FEATURE_MONITORING_CONFIG_CREATE_API)

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        detection_window_config = config_engine.build_monitoring_window_config(
            window_config_type="INSERT",
            time_offset="1w",
            window_length="1d",
        )

        # Act
        config_engine.enable_descriptive_statistics_monitoring(
            name=DEFAULT_NAME,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_window_config=detection_window_config,
            description=DEFAULT_DESCRIPTION,
            scheduler_config=DEFAULT_SCHEDULER_CONFIG,
        )

        # Assert
        config = mock_config_api.call_args[1]["fm_config"]
        assert config._feature_store_id == DEFAULT_FEATURE_STORE_ID
        assert config._feature_group_id == DEFAULT_FEATURE_GROUP_ID
        assert config._feature_view_id is None
        assert config._feature_name == DEFAULT_FEATURE_NAME
        assert config._enabled is True
        assert config._name == DEFAULT_NAME
        assert config._description == DEFAULT_DESCRIPTION
        assert config._feature_monitoring_type == "SCHEDULED_STATISTICS"
        assert config._detection_window_config.window_config_type == "INSERT"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"

        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    def test_enable_descriptive_statistics_monitoring_fv(self, mocker):
        # Arrange
        mock_config_api = mocker.patch(DEFAULT_FEATURE_MONITORING_CONFIG_CREATE_API)

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_id=DEFAULT_FEATURE_VIEW_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )

        detection_window_config = config_engine.build_monitoring_window_config(
            window_config_type="BATCH",
            time_offset="1w",
            window_length="1d",
        )

        # Act
        config_engine.enable_descriptive_statistics_monitoring(
            name=DEFAULT_NAME,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_window_config=detection_window_config,
            scheduler_config=DEFAULT_SCHEDULER_CONFIG,
        )

        # Assert
        config = mock_config_api.call_args[1]["fm_config"]
        assert config._feature_store_id == DEFAULT_FEATURE_STORE_ID
        assert config._feature_group_id is None
        assert config._feature_view_id == DEFAULT_FEATURE_VIEW_ID
        assert config._feature_name == DEFAULT_FEATURE_NAME
        assert config._enabled is True
        assert config._name == DEFAULT_NAME
        assert config._description is None
        assert config._feature_monitoring_type == "SCHEDULED_STATISTICS"
        assert config._detection_window_config.window_config_type == "BATCH"
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"

        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    # TODO: Add unit test for the run_feature_monitoring methods when more stable
