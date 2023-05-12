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

from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_config_engine
from hsfs.core.job_scheduler import JobScheduler
from hsfs.core.monitoring_window_config import WindowConfigType
from hsfs.util import convert_event_time_to_timestamp
from hsfs.constructor import query

from hsfs import feature_group, feature_view

from datetime import datetime, timedelta
from unittest.mock import call


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
DEFAULT_SCHEDULER_CONFIG = {
    "job_frequency": "HOURLY",
    "start_date_time": 1676457000,
    "enabled": True,
}
DEFAULT_TRANSFORMATION_FUNCTION_DATASET_VERSION = 2


class TestFeatureMonitoringConfigEngine:
    def test_build_stats_monitoring_only_config(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        detection_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type=WindowConfigType.ROLLING_TIME,
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
        assert (
            config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_MONITORING
        )
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"

        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    def test_build_feature_monitoring_config(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        detection_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type=WindowConfigType.ROLLING_TIME,
            time_offset="1w",
            window_length="1d",
            row_percentage=1.0,
        )
        reference_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type=WindowConfigType.SPECIFIC_VALUE, specific_value=2
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
        )

        # Assert
        assert config._feature_store_id == DEFAULT_FEATURE_STORE_ID
        assert config._feature_group_id == DEFAULT_FEATURE_GROUP_ID
        assert config._feature_name == DEFAULT_FEATURE_NAME
        assert config._name == DEFAULT_NAME
        assert config._description == DEFAULT_DESCRIPTION
        assert config._enabled is True
        assert (
            config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_COMPARISON
        )
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._detection_window_config.row_percentage == 1.0
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.SPECIFIC_VALUE
        )
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

        detection_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type=WindowConfigType.ROLLING_TIME,
            time_offset="1w",
            window_length="1d",
        )
        reference_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type=WindowConfigType.SPECIFIC_VALUE, specific_value=2
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
        assert (
            config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_COMPARISON
        )
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.SPECIFIC_VALUE
        )
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
        mocker.patch("hsfs.client.get_instance")

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_id=DEFAULT_FEATURE_VIEW_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )

        detection_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type=WindowConfigType.ROLLING_TIME,
            time_offset="1w",
            window_length="1d",
        )
        reference_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type=WindowConfigType.TRAINING_DATASET, training_dataset_id=12
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
        assert (
            config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_COMPARISON
        )
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.TRAINING_DATASET
        )
        assert config._reference_window_config.training_dataset_id == 12
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

    def test_enable_descriptive_statistics_monitoring_fg(self, mocker):
        # Arrange
        mock_config_api = mocker.patch(DEFAULT_FEATURE_MONITORING_CONFIG_CREATE_API)

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        detection_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type="ROLLING_TIME",
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
        assert (
            config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_MONITORING
        )
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"

        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    def test_enable_descriptive_statistics_monitoring_fv(self, mocker):
        # Arrange
        mock_config_api = mocker.patch(DEFAULT_FEATURE_MONITORING_CONFIG_CREATE_API)
        mocker.patch("hsfs.client.get_instance")

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_id=DEFAULT_FEATURE_VIEW_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )

        detection_window_config = config_engine._monitoring_window_config_engine.build_monitoring_window_config(
            window_config_type=WindowConfigType.ROLLING_TIME,
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
        assert (
            config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_MONITORING
        )
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"

        assert isinstance(config._scheduler_config, JobScheduler)
        assert config._scheduler_config.job_frequency == "HOURLY"
        assert config._scheduler_config.enabled is True
        assert config._scheduler_config.start_date_time == 1676457000000

    # TODO: Add unit test for the run_feature_monitoring methods when more stable
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
        assert config._feature_view_id == default_config._feature_view_id
        assert config.feature_name == default_config._feature_name is None
        assert config.enabled == default_config._enabled is True
        assert config.name == DEFAULT_NAME
        assert config.description == default_config._description is None
        assert (
            config._feature_monitoring_type
            == default_config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_MONITORING
        )
        assert (
            config.detection_window_config.window_config_type
            == default_config.detection_window_config.window_config_type
            == WindowConfigType.ALL_TIME
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
            config.scheduler_config.job_frequency
            == default_config.scheduler_config.job_frequency
            == "DAILY"
        )
        assert (
            convert_event_time_to_timestamp(time_before)
            <= config.scheduler_config.start_date_time
            <= convert_event_time_to_timestamp(time_after)
        )

    def test_fetch_feature_group_data(self, mocker, backend_fixtures):
        # Arrange
        unit_test_fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        select_mock = mocker.patch(
            "hsfs.feature_group.FeatureGroup.select",
            return_value=query.Query.from_response_json(
                backend_fixtures["query"]["get"]["response"]
            ),
        )
        as_of_mock = mocker.patch(
            "hsfs.constructor.query.Query.as_of",
            return_value=query.Query.from_response_json(
                backend_fixtures["query"]["get"]["response"]
            ),
        )
        read_mock = mocker.patch("hsfs.constructor.query.Query.read")
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        start_time = (datetime.now() - timedelta(days=1)).timestamp()
        end_time = datetime.now().timestamp()

        # Act
        _ = config_engine.fetch_feature_group_data(
            entity=unit_test_fg,
            feature_name=None,
            start_time=None,
            end_time=None,
        )
        _ = config_engine.fetch_feature_group_data(
            entity=unit_test_fg,
            feature_name=DEFAULT_FEATURE_NAME,
            start_time=start_time,
            end_time=end_time,
        )
        _ = config_engine.fetch_feature_group_data(
            entity=unit_test_fg,
            feature_name=None,
            start_time=start_time,
            end_time=None,
        )
        _ = config_engine.fetch_feature_group_data(
            entity=unit_test_fg,
            feature_name=DEFAULT_FEATURE_NAME,
            start_time=None,
            end_time=end_time,
        )

        # Assert
        assert select_mock.call_count == 2
        as_of_mock.assert_has_calls(
            [
                call(exclude_until=None, wallclock_time=None),
                call(exclude_until=start_time, wallclock_time=end_time),
                call(exclude_until=start_time, wallclock_time=None),
                call(exclude_until=None, wallclock_time=end_time),
            ],
            any_order=False,
        )
        assert read_mock.call_count == 4

    def test_fetch_feature_view_data(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.client.get_instance")
        unit_test_fv = feature_view.FeatureView.from_response_json(
            backend_fixtures["feature_view"]["get"]["response"]
        )
        mock_vector_server = mocker.patch(
            "hsfs.core.vector_server.VectorServer",
        )
        # if use use_event_time is true, use get_batch_query for event time
        get_batch_query_mock = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query",
            return_value=query.Query.from_response_json(
                backend_fixtures["query"]["get"]["response"]
            ),
        )
        # if use_event_time is false, use as of for commit time
        as_of_mock = mocker.patch(
            "hsfs.constructor.query.Query.as_of",
            return_value=query.Query.from_response_json(
                backend_fixtures["query"]["get"]["response"]
            ),
        )
        read_mock = mocker.patch("hsfs.constructor.query.Query.read")

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_id=unit_test_fv._id,
            feature_view_name=unit_test_fv.name,
            feature_view_version=unit_test_fv.version,
        )
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        # Act
        # 4 possibilities : with or without use_event_time, with or without transformation_function_dataset_version
        # use as_of
        _ = config_engine.fetch_feature_view_data(
            entity=unit_test_fv,
            feature_name=None,
            start_time=None,
            end_time=None,
            use_event_time=False,
            transformation_function_dataset_version=None,
        )
        # use get_batch_query
        _ = config_engine.fetch_feature_view_data(
            entity=unit_test_fv,
            feature_name=DEFAULT_FEATURE_NAME,
            start_time=start_time,
            end_time=None,
            use_event_time=True,
            transformation_function_dataset_version=None,
        )
        # use as_of
        _ = config_engine.fetch_feature_view_data(
            entity=unit_test_fv,
            feature_name=None,
            start_time=start_time,
            end_time=end_time,
            use_event_time=False,
            transformation_function_dataset_version=DEFAULT_TRANSFORMATION_FUNCTION_DATASET_VERSION,
        )
        # use get_batch_query
        _ = config_engine.fetch_feature_view_data(
            entity=unit_test_fv,
            feature_name=DEFAULT_FEATURE_NAME,
            start_time=None,
            end_time=end_time,
            use_event_time=True,
            transformation_function_dataset_version=DEFAULT_TRANSFORMATION_FUNCTION_DATASET_VERSION
            + 1,
        )

        # Assert
        assert read_mock.call_count == 4
        print(mock_vector_server.call_args_list)
        get_batch_query_mock.assert_has_calls(
            [
                call(
                    feature_view_obj=unit_test_fv,
                    start_time=start_time,
                    end_time=None,
                    with_label=False,
                    training_dataset_version=None,
                ),
                call(
                    feature_view_obj=unit_test_fv,
                    start_time=None,
                    end_time=end_time,
                    with_label=False,
                    training_dataset_version=DEFAULT_TRANSFORMATION_FUNCTION_DATASET_VERSION
                    + 1,
                ),
            ],
            any_order=False,
        )
        as_of_mock.assert_has_calls(
            [
                call(exclude_until=None, wallclock_time=None),
                call(exclude_until=start_time, wallclock_time=end_time),
            ],
            any_order=False,
        )
        mock_vector_server.assert_has_calls(
            [
                call(
                    5,
                    [mocker.ANY, mocker.ANY],
                    DEFAULT_TRANSFORMATION_FUNCTION_DATASET_VERSION,
                ),
                call(
                    5,
                    [mocker.ANY, mocker.ANY],
                    DEFAULT_TRANSFORMATION_FUNCTION_DATASET_VERSION + 1,
                ),
            ],
            any_order=True,
        )
