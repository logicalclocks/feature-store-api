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

from hsfs.core import feature_monitoring_result_engine
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core import feature_monitoring_config as fmc
from datetime import datetime, date, timedelta
import dateutil
from hsfs import util
from hsfs.core import execution

DEFAULT_MONITORING_TIME_SORT_BY = "monitoring_time:desc"
DEFAULT_FEATURE_STORE_ID = 67
DEFAULT_FEATURE_GROUP_ID = 13
DEFAULT_FEATURE_VIEW_ID = 22
DEFAULT_FEATURE_VIEW_NAME = "test_feature_view"
DEFAULT_FEATURE_VIEW_VERSION = 2
DEFAULT_CONFIG_ID = 32
DEFAULT_FEATURE_NAME = "amount"

FEATURE_MONITORING_CONFIG_CREATE_API = (
    "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi.create"
)
GET_JOB_API = "hsfs.core.job_api.JobApi.get"
LAST_EXECUTION_API = "hsfs.core.job_api.JobApi.last_execution"


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
            feature_view_id=DEFAULT_FEATURE_VIEW_ID,
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

    def test_save_feature_monitoring_result_via_fg(self, mocker, backend_fixtures):
        # Arrange
        execution_id = 123
        shift_detected = False
        difference = 0.3

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

        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.save_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            execution_id=execution_id,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            difference=difference,
            shift_detected=shift_detected,
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id == 52
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_save_feature_monitoring_result_detection_stats_only_via_fg(
        self, mocker, backend_fixtures
    ):
        # Arrange
        execution_id = 123
        shift_detected = False
        difference = 0.3

        detection_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.save_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            execution_id=execution_id,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_statistics=detection_statistics,
            reference_statistics=None,
            difference=difference,
            shift_detected=shift_detected,
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id is None
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_save_feature_monitoring_result_via_fv(self, mocker, backend_fixtures):
        # Arrange
        execution_id = 123
        shift_detected = False
        difference = 0.3

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

        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_id=DEFAULT_FEATURE_VIEW_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.save_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            execution_id=execution_id,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            difference=difference,
            shift_detected=shift_detected,
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id == 52
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_save_feature_monitoring_result_detection_stats_only_via_fv(
        self, mocker, backend_fixtures
    ):
        # Arrange
        execution_id = 123
        shift_detected = False
        difference = 0.3

        detection_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_id=DEFAULT_FEATURE_VIEW_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.save_feature_monitoring_result(
            config_id=DEFAULT_CONFIG_ID,
            execution_id=execution_id,
            feature_name=DEFAULT_FEATURE_NAME,
            detection_statistics=detection_statistics,
            reference_statistics=None,
            difference=difference,
            shift_detected=shift_detected,
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id is None
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    # Run and save statistics comparison

    def test_run_and_save_statistics_comparison_single_feature(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )
        mock_get_job_api = mocker.patch(
            GET_JOB_API, return_value=backend_fixtures["job"]["get"]["response"]
        )
        mock_last_exec_job_api = mocker.patch(
            LAST_EXECUTION_API,
            return_value=execution.Execution.from_response_json(
                backend_fixtures["execution"]["get"]["response"]
            ),
        )

        execution_id = 123
        shift_detected = False
        difference = 0

        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["detection_insert_reference_snapshot"]["response"]
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

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

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.run_and_save_statistics_comparison(
            config, detection_statistics, reference_statistics
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        mock_get_job_api.assert_called_once_with(config.job_name)
        mock_last_exec_job_api.assert_called_once()
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id == 52
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_run_and_save_statistics_comparison_multiple_features(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )
        mock_get_job_api = mocker.patch(
            GET_JOB_API, return_value=backend_fixtures["job"]["get"]["response"]
        )
        mock_last_exec_job_api = mocker.patch(
            LAST_EXECUTION_API,
            return_value=execution.Execution.from_response_json(
                backend_fixtures["execution"]["get"]["response"]
            ),
        )

        execution_id = 123
        shift_detected = False
        difference = 0

        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["detection_insert_reference_snapshot"]["response"]
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

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

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.run_and_save_statistics_comparison(
            config, [detection_statistics], [reference_statistics]
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        mock_get_job_api.assert_called_once_with(config.job_name)
        mock_last_exec_job_api.assert_called_once()
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id == 52
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_run_and_save_statistics_comparison_reference_value_single_feature(
        self, mocker, backend_fixtures
    ):
        # Arrange
        execution_id = 123
        shift_detected = False
        difference = 0
        reference_specific_value = 5.1

        mock_get_job_api = mocker.patch(
            GET_JOB_API, return_value=backend_fixtures["job"]["get"]["response"]
        )
        mock_last_exec_job_api = mocker.patch(
            LAST_EXECUTION_API,
            return_value=execution.Execution.from_response_json(
                backend_fixtures["execution"]["get"]["response"]
            ),
        )

        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["detection_insert_reference_specific_value"]["response"]
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        detection_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.run_and_save_statistics_comparison(
            config, detection_statistics, reference_specific_value
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        mock_get_job_api.assert_called_once_with(config.job_name)
        mock_last_exec_job_api.assert_called_once()
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id is None
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_run_and_save_statistics_comparison_reference_value_multiple_features(
        self, mocker, backend_fixtures
    ):
        # Arrange
        execution_id = 123
        shift_detected = False
        difference = 0
        reference_specific_value = 5.1

        mock_get_job_api = mocker.patch(
            GET_JOB_API, return_value=backend_fixtures["job"]["get"]["response"]
        )
        mock_last_exec_job_api = mocker.patch(
            LAST_EXECUTION_API,
            return_value=execution.Execution.from_response_json(
                backend_fixtures["execution"]["get"]["response"]
            ),
        )

        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["detection_insert_reference_specific_value"]["response"]
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        detection_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.run_and_save_statistics_comparison(
            config, [detection_statistics], reference_specific_value
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        mock_get_job_api.assert_called_once_with(config.job_name)
        mock_last_exec_job_api.assert_called_once()
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id is None
        assert result._detection_statistics is None
        assert result._reference_statistics is None
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_run_and_save_statistics_comparison_scheduled_stats_only(
        self, mocker, backend_fixtures
    ):
        # Arrange
        execution_id = 123
        reference_statistics = None

        mock_get_job_api = mocker.patch(
            GET_JOB_API, return_value=backend_fixtures["job"]["get"]["response"]
        )
        mock_last_exec_job_api = mocker.patch(
            LAST_EXECUTION_API,
            return_value=execution.Execution.from_response_json(
                backend_fixtures["execution"]["get"]["response"]
            ),
        )

        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["detection_insert_scheduled_stats_only"]["response"]
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        detection_stats_json = backend_fixtures["feature_descriptive_statistics"][
            "get_fractional_feature_statistics"
        ]["response"]
        detection_statistics = [
            FeatureDescriptiveStatistics.from_response_json(detection_stats_json)
        ]

        mock_result_api = mocker.patch(
            FEATURE_MONITORING_CONFIG_CREATE_API,
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        before_time = datetime.now() - timedelta(seconds=1)

        # Act
        result_engine.run_and_save_statistics_comparison(
            config, detection_statistics, reference_statistics
        )
        after_time = datetime.now() + timedelta(seconds=1)

        # Assert
        result = mock_result_api.call_args[0][0]
        mock_get_job_api.assert_called_once_with(config.job_name)
        mock_last_exec_job_api.assert_called_once()
        assert result._config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._feature_name == DEFAULT_FEATURE_NAME
        assert result._detection_statistics_id == 52
        assert result._reference_statistics_id is None
        assert result._reference_statistics_id is None
        assert result._reference_statistics is None
        assert result._difference is None
        assert result._shift_detected is False
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

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

    # TODO: Add tests for computing_difference functions:
    # - compute_difference_and_shift
    # - compute_difference_between_stats
    # - compute_difference_between_specific_values
