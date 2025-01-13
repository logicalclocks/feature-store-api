#
#   Copyright 2024 Hopsworks AB
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

from datetime import datetime

from hsfs import util
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_config_engine
from hsfs.core import monitoring_window_config as mwc


DEFAULT_DESCRIPTION = "A feature monitoring configuration for unit test."
DEFAULT_NAME = "test_monitoring_config"
DEFAULT_FEATURE_NAME = "monitored_feature"
DEFAULT_FEATURE_MONITORING_CONFIG_CREATE_API = (
    "hsfs.core.feature_monitoring_config_api.FeatureMonitoringConfigApi.create"
)
DEFAULT_FEATURE_MONITORING_CONFIG_SETUP_JOB_API = "hsfs.core.feature_monitoring_config_api.FeatureMonitoringConfigApi.setup_feature_monitoring_job"
DEFAULT_FEATURE_STORE_ID = 67
DEFAULT_FEATURE_GROUP_ID = 13
DEFAULT_FEATURE_VIEW_NAME = "feature_view_unittest"
DEFAULT_FEATURE_VIEW_VERSION = 2
DEFAULT_JOB_SCHEDULE = {
    "cron_expression": "0 0 * ? * * *",
    "start_date_time": 1676457000,
    "enabled": True,
}


class TestFeatureMonitoringConfigEngine:
    def test_build_default_statistics_monitoring_config(self, backend_fixtures):
        # Arrange
        default_config = fmc.FeatureMonitoringConfig.from_response_json(
            backend_fixtures["feature_monitoring_config"][
                "default_statistics_monitoring_config"
            ]
        )
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        time_before = datetime.now()
        config = config_engine._build_default_statistics_monitoring_config(
            name=DEFAULT_NAME,
        )
        time_after = datetime.now()

        # Assert
        assert config._feature_store_id == default_config._feature_store_id
        assert config._feature_group_id == default_config._feature_group_id
        assert config._feature_view_name == default_config._feature_view_name
        assert config._feature_view_version == default_config._feature_view_version
        assert config.feature_name == default_config._feature_name is None
        assert config.enabled == default_config.enabled is True
        assert config.name == DEFAULT_NAME
        assert config.description == default_config._description is None
        assert (
            config._feature_monitoring_type
            == default_config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_COMPUTATION
        )
        assert (
            config.detection_window_config.window_config_type
            == default_config.detection_window_config.window_config_type
            == mwc.WindowConfigType.ALL_TIME
        )
        assert (
            config.detection_window_config.time_offset
            == default_config.detection_window_config.time_offset
            is None
        )
        assert (
            config.detection_window_config.window_length
            == default_config.detection_window_config.window_length
            is None
        )
        assert (
            config.detection_window_config.row_percentage
            == default_config.detection_window_config.row_percentage
            == 1.0
        )
        assert (
            config.job_schedule.cron_expression
            == default_config.job_schedule.cron_expression
            == "0 0 12 ? * * *"
        )

        assert (
            util.convert_event_time_to_timestamp(time_before)
            <= util.convert_event_time_to_timestamp(config.job_schedule.start_date_time)
            <= util.convert_event_time_to_timestamp(time_after)
        )
