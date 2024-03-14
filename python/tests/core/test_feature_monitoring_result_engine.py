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

from datetime import date, datetime, timedelta
from unittest.mock import call

import dateutil
from hsfs import util
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_result_engine
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


DEFAULT_MONITORING_TIME_SORT_BY = "monitoring_time:desc"
DEFAULT_FEATURE_STORE_ID = 67
DEFAULT_FEATURE_GROUP_ID = 13
DEFAULT_FEATURE_VIEW_NAME = "test_feature_view"
DEFAULT_FEATURE_VIEW_VERSION = 2
DEFAULT_CONFIG_ID = 32
DEFAULT_FEATURE_NAME = "amount"
DEFAULT_JOB_NAME = "test_job"
DEFAULT_SPECIFIC_VALUE = 2.01
DEFAULT_DIFFERENCE = 6.5

FEATURE_MONITORING_RESULT_CREATE_API = (
    "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi.create"
)
GET_JOB_API = "hsfs.core.job_api.JobApi.get"
LAST_EXECUTION_API = "hsfs.core.job_api.JobApi.last_execution"
HSFS_CLIENT_GET_INSTANCE = "hsfs.client.get_instance"


class TestFeatureMonitoringResultEngine:
    # Fetch results

    def test_fetch_all_feature_monitoring_results_by_config_id_via_fg(self, mocker):
        # Arrange
        start_time = "2022-01-01 10:10:10"
        end_time = "2022-02-02 20:20:20"

        mock_result_api = mocker.patch(
            "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi.get_by_config_id",
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        result_engine.fetch_all_feature_monitoring_results_by_config_id(
            config_id=DEFAULT_CONFIG_ID,
            start_time=start_time,
            end_time=end_time,
        )

        # Assert
        assert mock_result_api.call_args[1]["config_id"] == DEFAULT_CONFIG_ID
        assert isinstance(mock_result_api.call_args[1]["query_params"], dict)
        assert (
            mock_result_api.call_args[1]["query_params"]["filter_by"][0]
            == "monitoring_time_gte:1641031810000"
        )
        assert (
            mock_result_api.call_args[1]["query_params"]["filter_by"][1]
            == "monitoring_time_lte:1643833220000"
        )
        assert (
            mock_result_api.call_args[1]["query_params"]["sort_by"]
            == DEFAULT_MONITORING_TIME_SORT_BY
        )

    def test_fetch_all_feature_monitoring_results_by_config_id_via_fv(self, mocker):
        # Arrange
        start_time = "2022-01-01 01:01:01"
        end_time = "2022-02-02 02:02:02"

        mock_result_api = mocker.patch(
            "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi.get_by_config_id",
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )

        # Act
        result_engine.fetch_all_feature_monitoring_results_by_config_id(
            config_id=DEFAULT_CONFIG_ID,
            start_time=start_time,
            end_time=end_time,
        )

        # Assert
        assert mock_result_api.call_args[1]["config_id"] == DEFAULT_CONFIG_ID
        assert isinstance(mock_result_api.call_args[1]["query_params"], dict)
        assert (
            mock_result_api.call_args[1]["query_params"]["filter_by"][0]
            == "monitoring_time_gte:1640998861000"
        )
        assert (
            mock_result_api.call_args[1]["query_params"]["filter_by"][1]
            == "monitoring_time_lte:1643767322000"
        )
        assert (
            mock_result_api.call_args[1]["query_params"]["sort_by"]
            == DEFAULT_MONITORING_TIME_SORT_BY
        )

    # Save results

    def test_save_feature_monitoring_result_via_fg(self, mocker):
        # Arrange
        result_create_api_mock = mocker.patch(
            FEATURE_MONITORING_RESULT_CREATE_API,
        )
        mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine.get_monitoring_job_execution_id",
        )
        result_engine = result_engine = (
            feature_monitoring_result_engine.FeatureMonitoringResultEngine(
                feature_store_id=DEFAULT_FEATURE_STORE_ID,
                feature_group_id=DEFAULT_FEATURE_GROUP_ID,
            )
        )
        result = result_engine.build_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            feature_name=DEFAULT_FEATURE_NAME,
        )

        # Act
        result_engine.save_feature_monitoring_result(result)

        # Assert
        assert result_create_api_mock.call_args[0][0] == result

    def test_save_feature_monitoring_result_via_fv(self, mocker, backend_fixtures):
        # Arrange
        result_create_api_mock = mocker.patch(
            FEATURE_MONITORING_RESULT_CREATE_API,
        )
        mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine.get_monitoring_job_execution_id",
        )
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )
        result = result_engine.build_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            feature_name=DEFAULT_FEATURE_NAME,
        )

        # Act
        result_engine.save_feature_monitoring_result(result)

        # Assert
        assert result_create_api_mock.call_args[0][0] == result

    # Build result
    def test_build_statistics_monitoring_result_no_reference_stats(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mocker.patch(HSFS_CLIENT_GET_INSTANCE)
        job_api_last_execution_mock = mocker.patch(
            LAST_EXECUTION_API,
        )
        job_api_get_mock = mocker.patch(GET_JOB_API)
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        before_time = util.convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        result = result_engine.build_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_statistics=FeatureDescriptiveStatistics.from_response_json(
                backend_fixtures["feature_descriptive_statistics"][
                    "get_fractional_feature_statistics"
                ]["response"]
            ),
            job_name=DEFAULT_JOB_NAME,
        )
        after_time = util.convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id is None
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference is None
        assert result._shift_detected is False
        assert after_time >= result._monitoring_time >= before_time
        job_api_get_mock.assert_called_once_with(name=DEFAULT_JOB_NAME)
        job_api_last_execution_mock.assert_called_once()

    def test_build_feature_monitoring_result_with_referece_statistics(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mocker.patch(HSFS_CLIENT_GET_INSTANCE)
        job_api_last_execution_mock = mocker.patch(
            LAST_EXECUTION_API,
        )
        job_api_get_mock = mocker.patch(GET_JOB_API)
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        before_time = util.convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        result = result_engine.build_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_statistics=FeatureDescriptiveStatistics.from_response_json(
                backend_fixtures["feature_descriptive_statistics"][
                    "get_fractional_feature_statistics"
                ]["response"]
            ),
            reference_statistics=FeatureDescriptiveStatistics.from_response_json(
                backend_fixtures["feature_descriptive_statistics"][
                    "get_fractional_feature_statistics"
                ]["response"]
            ),
            job_name=DEFAULT_JOB_NAME,
            difference=DEFAULT_DIFFERENCE,
        )
        after_time = util.convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id == 52
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == DEFAULT_DIFFERENCE
        assert result._shift_detected is False
        assert result._specific_value is None
        assert result._empty_detection_window is False
        assert result._empty_reference_window is False
        assert result._raised_exception is False
        assert after_time >= result._monitoring_time >= before_time
        job_api_get_mock.assert_called_once_with(name=DEFAULT_JOB_NAME)
        job_api_last_execution_mock.assert_called_once()

    def test_build_feature_monitoring_result_with_specific_value(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mocker.patch(HSFS_CLIENT_GET_INSTANCE)
        job_api_last_execution_mock = mocker.patch(
            LAST_EXECUTION_API,
        )
        job_api_get_mock = mocker.patch(GET_JOB_API)
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        before_time = util.convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        result = result_engine.build_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_statistics=FeatureDescriptiveStatistics.from_response_json(
                backend_fixtures["feature_descriptive_statistics"][
                    "get_fractional_feature_statistics"
                ]["response"]
            ),
            specific_value=DEFAULT_SPECIFIC_VALUE,
            job_name=DEFAULT_JOB_NAME,
            difference=DEFAULT_DIFFERENCE,
            shift_detected=True,
        )
        after_time = util.convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id is None
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == DEFAULT_DIFFERENCE
        assert result._shift_detected is True
        assert result._specific_value == DEFAULT_SPECIFIC_VALUE
        assert result._empty_detection_window is False
        assert result._empty_reference_window is True
        assert result._raised_exception is False
        assert after_time >= result._monitoring_time >= before_time
        job_api_get_mock.assert_called_once_with(name=DEFAULT_JOB_NAME)
        job_api_last_execution_mock.assert_called_once()

    def test_save_feature_monitoring_result_with_exception(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mocker.patch(HSFS_CLIENT_GET_INSTANCE)
        job_api_last_execution_mock = mocker.patch(
            LAST_EXECUTION_API,
        )
        job_api_get_mock = mocker.patch(GET_JOB_API)
        result_engine_save_mock = mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine.save_feature_monitoring_result",
        )
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        before_time = util.convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        result_engine.save_feature_monitoring_result_with_exception(
            config_id=DEFAULT_CONFIG_ID,
            job_name=DEFAULT_JOB_NAME,
        )
        after_time = util.convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        result = result_engine_save_mock.call_args[1]["result"]
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._feature_name == ""
        assert result._detection_statistics_id is None
        assert result._reference_statistics_id is None
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference is None
        assert result._shift_detected is False
        assert result._specific_value is None
        assert result._empty_detection_window is True
        assert result._empty_reference_window is True
        assert result._raised_exception is True
        assert after_time >= result._monitoring_time >= before_time
        job_api_get_mock.assert_called_once_with(name=DEFAULT_JOB_NAME)
        job_api_last_execution_mock.assert_called_once()

    # Helper methods

    def test_build_query_params_time_none(self):
        # Arrange
        start_time = None
        end_time = None

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert "filter_by" not in query_params
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_datetime(self):
        # Arrange
        start_time = dateutil.parser.parse("2022-01-01T01:01:01Z")
        end_time = dateutil.parser.parse("2022-02-02T02:02:02Z")
        start_timestamp = 1640998861000
        end_timestamp = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_date(self):
        # Arrange
        start_time = date(year=2022, month=1, day=1)
        end_time = date(year=2022, month=2, day=2)
        start_timestamp = 1640995200000
        end_timestamp = 1643760000000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_str(self):
        # Arrange
        start_time = "2022-01-01 01:01:01"
        end_time = "2022-02-02 02:02:02"
        start_timestamp = 1640998861000
        end_timestamp = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_int(self):
        # Arrange
        start_time = 1640998861000
        end_time = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_time}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_time}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_int_with_statistics(self):
        # Arrange
        start_time = 1640998861000
        end_time = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=True
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_time}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_time}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY
        assert query_params["expand"] == "statistics"

    # Feature monitoring job utils
    def test_compute_difference_between_specific_values(self):
        # Arrange
        detection_specific_value = 25
        reference_specific_value = 50
        expected_difference = 25
        expected_relative_difference = 0.5
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        absolute_difference = result_engine.compute_difference_between_specific_values(
            detection_value=detection_specific_value,
            reference_value=reference_specific_value,
            relative=False,
        )
        relative_difference = result_engine.compute_difference_between_specific_values(
            detection_value=detection_specific_value,
            reference_value=reference_specific_value,
            relative=True,
        )

        # Assert
        assert absolute_difference == expected_difference
        assert relative_difference == expected_relative_difference

    def test_compute_difference_between_stats(self, backend_fixtures):
        # Arrange
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        detection_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )
        reference_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )
        reference_statistics._count += 6

        # Act
        mean_difference = result_engine.compute_difference_between_stats(
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            metric="mean",
            relative=False,
            specific_value=None,
        )
        count_relative_difference = result_engine.compute_difference_between_stats(
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            metric="count",
            relative=True,
            specific_value=None,
        )
        count_specific_difference = result_engine.compute_difference_between_stats(
            detection_statistics=detection_statistics,
            reference_statistics=None,
            metric="count",
            relative=False,
            specific_value=2,
        )

        # Assert
        assert mean_difference == 0
        assert count_relative_difference == 0.6
        assert count_specific_difference == 2

    def test_compute_difference_and_shift(self, mocker, backend_fixtures):
        # Arrange
        compute_stats_difference_mock = mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine.compute_difference_between_stats",
            return_value=DEFAULT_DIFFERENCE,
        )
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        fm_config = fmc.FeatureMonitoringConfig.from_response_json(
            backend_fixtures["feature_monitoring_config"]["get_via_feature_group"][
                "detection_insert_reference_snapshot"
            ]["response"]
        )
        detection_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )
        reference_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        # Act
        fm_config._statistics_comparison_config["threshold"] = DEFAULT_DIFFERENCE + 1
        (
            difference_no_shift,
            no_shift_detected,
        ) = result_engine.compute_difference_and_shift(
            fm_config=fm_config,
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            specific_value=None,
        )
        fm_config._statistics_comparison_config["threshold"] = DEFAULT_DIFFERENCE - 1
        (
            difference_with_shift,
            with_shift_detected,
        ) = result_engine.compute_difference_and_shift(
            fm_config=fm_config,
            detection_statistics=detection_statistics,
            reference_statistics=None,
            specific_value=10,
        )

        # Assert
        assert difference_no_shift == DEFAULT_DIFFERENCE
        assert no_shift_detected is False
        assert difference_with_shift == DEFAULT_DIFFERENCE
        assert with_shift_detected is True
        compute_stats_difference_mock.assert_has_calls(
            [
                call(
                    detection_statistics=detection_statistics,
                    reference_statistics=reference_statistics,
                    metric=fm_config.statistics_comparison_config.get("metric").lower(),
                    relative=fm_config.statistics_comparison_config.get("relative"),
                    specific_value=None,
                ),
                call(
                    detection_statistics=detection_statistics,
                    reference_statistics=None,
                    metric=fm_config.statistics_comparison_config.get("metric").lower(),
                    relative=fm_config.statistics_comparison_config.get("relative"),
                    specific_value=10,
                ),
            ],
            any_order=False,
        )

    def test_run_and_save_statistics_comparison_single_detection_stats(
        self, mocker, backend_fixtures
    ):
        # Arrange
        result_engine_save_mock = mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine.save_feature_monitoring_result",
        )
        result_engine_build_mock = mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine.build_feature_monitoring_result",
        )
        result_engine_compute_mock = mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine.compute_difference_and_shift",
            return_value=(DEFAULT_DIFFERENCE, True),
        )
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        detection_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )
        detection_statistics_categorical = (
            FeatureDescriptiveStatistics.from_response_json(
                backend_fixtures["feature_descriptive_statistics"][
                    "get_string_feature_statistics"
                ]["response"]
            )
        )
        reference_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )
        fm_config = fmc.FeatureMonitoringConfig.from_response_json(
            backend_fixtures["feature_monitoring_config"]["get_via_feature_group"][
                "detection_insert_reference_snapshot"
            ]["response"]
        )

        # Act
        result_engine.run_and_save_statistics_comparison(
            fm_config=fm_config,
            detection_statistics=[detection_statistics],
            reference_statistics=None,
            specific_value=None,
        )
        result_engine.run_and_save_statistics_comparison(
            fm_config=fm_config,
            detection_statistics=[
                detection_statistics,
                detection_statistics_categorical,
            ],
            reference_statistics=None,
            specific_value=None,
        )
        result_engine.run_and_save_statistics_comparison(
            fm_config=fm_config,
            detection_statistics=[detection_statistics],
            reference_statistics=[reference_statistics],
            specific_value=None,
        )
        result_engine.run_and_save_statistics_comparison(
            fm_config=fm_config,
            detection_statistics=[detection_statistics_categorical],
            reference_statistics=None,
            specific_value=10,
        )

        # Assert
        assert result_engine_compute_mock.call_count == 2
        result_engine_compute_mock.assert_has_calls(
            [
                call(
                    fm_config=fm_config,
                    detection_statistics=detection_statistics,
                    reference_statistics=reference_statistics,
                ),
                call(
                    fm_config=fm_config,
                    detection_statistics=detection_statistics_categorical,
                    specific_value=10,
                ),
            ],
            any_order=False,
        )
        assert result_engine_build_mock.call_count == 5
        result_engine_build_mock.assert_has_calls(
            [
                call(
                    config_id=32,
                    feature_name="amount",
                    detection_statistics=detection_statistics,
                ),
                call(
                    config_id=32,
                    feature_name="amount",
                    detection_statistics=detection_statistics,
                ),
                call(
                    config_id=32,
                    feature_name="first_name",
                    detection_statistics=detection_statistics_categorical,
                ),
                call(
                    config_id=32,
                    feature_name="amount",
                    detection_statistics=detection_statistics,
                    reference_statistics=reference_statistics,
                    difference=6.5,
                    shift_detected=True,
                ),
                call(
                    config_id=32,
                    feature_name="first_name",
                    detection_statistics=detection_statistics_categorical,
                    specific_value=10,
                    difference=6.5,
                    shift_detected=True,
                ),
            ],
            any_order=False,
        )
        assert result_engine_save_mock.call_count == 5
