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
from datetime import datetime, date
import dateutil
from hsfs import util

DEFAULT_MONITORING_TIME_SORT_BY = "monitoring_time:desc"
DEFAULT_FEATURE_STORE_ID = 67
DEFAULT_FEATURE_GROUP_ID = 13
DEFAULT_FEATURE_VIEW_ID = 22
DEFAULT_FEATURE_VIEW_NAME = "test_feature_view"
DEFAULT_FEATURE_VIEW_VERSION = 2
DEFAULT_CONFIG_ID = 32


class TestFeatureMonitoringResultEngine:
    def test_fetch_all_feature_monitoring_results_by_config_id_via_fg(self, mocker):
        # Arrange
        start_time = "2022-01-01 10:10:10"
        end_time = "2022-02-02 20:20:20"

        mock_result_api = mocker.patch(
            "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi.get_by_config_id",
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID
        )

        # Act
        result_engine.fetch_all_feature_monitoring_results_by_config_id(
            config_id=DEFAULT_CONFIG_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
            start_time=start_time,
            end_time=end_time,
        )

        # Assert
        assert mock_result_api.call_args[1]["config_id"] == DEFAULT_CONFIG_ID
        assert (
            mock_result_api.call_args[1]["feature_group_id"] == DEFAULT_FEATURE_GROUP_ID
        )
        assert mock_result_api.call_args[1]["feature_view_name"] is None
        assert mock_result_api.call_args[1]["feature_view_version"] is None
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
            feature_store_id=DEFAULT_FEATURE_STORE_ID
        )

        # Act
        result_engine.fetch_all_feature_monitoring_results_by_config_id(
            config_id=DEFAULT_CONFIG_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
            start_time=start_time,
            end_time=end_time,
        )

        # Assert
        assert mock_result_api.call_args[1]["config_id"] == DEFAULT_CONFIG_ID
        assert mock_result_api.call_args[1]["feature_group_id"] is None
        assert (
            mock_result_api.call_args[1]["feature_view_name"]
            == DEFAULT_FEATURE_VIEW_NAME
        )
        assert (
            mock_result_api.call_args[1]["feature_view_version"]
            == DEFAULT_FEATURE_VIEW_VERSION
        )
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

    def test_save_feature_monitoring_result_via_fg(self, mocker):
        # Arrange
        execution_id = 123
        detection_stats_id = 333
        reference_stats_id = 222
        shift_detected = False
        difference = 0.3

        mock_result_api = mocker.patch(
            "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi.create",
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID
        )
        before_time = datetime.now()

        # Act
        result_engine.save_feature_monitoring_result(
            feature_monitoring_config_id=DEFAULT_CONFIG_ID,
            execution_id=execution_id,
            detection_stats_id=detection_stats_id,
            reference_stats_id=reference_stats_id,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
            difference=difference,
            shift_detected=shift_detected,
        )
        after_time = datetime.now()

        # Assert
        assert (
            mock_result_api.call_args[1]["feature_group_id"] == DEFAULT_FEATURE_GROUP_ID
        )
        assert mock_result_api.call_args[1]["feature_view_name"] is None
        assert mock_result_api.call_args[1]["feature_view_version"] is None
        result = mock_result_api.call_args[0][0]
        assert result._feature_monitoring_config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._detection_stats_id == detection_stats_id
        assert result._reference_stats_id == reference_stats_id
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_save_feature_monitoring_result_via_fv(self, mocker):
        # Arrange
        execution_id = 123
        detection_stats_id = 333
        reference_stats_id = 222
        shift_detected = False
        difference = 0.3

        mock_result_api = mocker.patch(
            "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi.create",
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID
        )
        before_time = datetime.now()

        # Act
        result_engine.save_feature_monitoring_result(
            feature_monitoring_config_id=DEFAULT_CONFIG_ID,
            execution_id=execution_id,
            detection_stats_id=detection_stats_id,
            reference_stats_id=reference_stats_id,
            feature_view_id=DEFAULT_FEATURE_VIEW_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
            difference=difference,
            shift_detected=shift_detected,
        )
        after_time = datetime.now()

        # Assert
        assert mock_result_api.call_args[1]["feature_group_id"] is None
        assert (
            mock_result_api.call_args[1]["feature_view_name"]
            == DEFAULT_FEATURE_VIEW_NAME
        )
        assert (
            mock_result_api.call_args[1]["feature_view_version"]
            == DEFAULT_FEATURE_VIEW_VERSION
        )
        result = mock_result_api.call_args[0][0]
        assert result._feature_monitoring_config_id == DEFAULT_CONFIG_ID
        assert result._execution_id == execution_id
        assert result._detection_stats_id == detection_stats_id
        assert result._reference_stats_id == reference_stats_id
        assert result._difference == difference
        assert result._shift_detected == shift_detected
        assert isinstance(result._monitoring_time, int)
        assert (
            util.convert_event_time_to_timestamp(before_time) <= result._monitoring_time
        )
        assert (
            util.convert_event_time_to_timestamp(after_time) >= result._monitoring_time
        )

    def test_build_query_params_none(self):
        # Arrange
        start_time = None
        end_time = None

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=67
        )

        # Act
        query_params = result_engine.build_query_params(
            start_time=start_time, end_time=end_time
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 0
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_datetime(self):
        # Arrange
        start_time = dateutil.parser.parse("2022-01-01T01:01:01Z")
        end_time = dateutil.parser.parse("2022-02-02T02:02:02Z")
        start_timestamp = 1640998861000
        end_timestamp = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=67
        )

        # Act
        query_params = result_engine.build_query_params(
            start_time=start_time, end_time=end_time
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_date(self):
        # Arrange
        start_time = date(year=2022, month=1, day=1)
        end_time = date(year=2022, month=2, day=2)
        start_timestamp = 1640995200000
        end_timestamp = 1643760000000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=67
        )

        # Act
        query_params = result_engine.build_query_params(
            start_time=start_time, end_time=end_time
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_str(self):
        # Arrange
        start_time = "2022-01-01 01:01:01"
        end_time = "2022-02-02 02:02:02"
        start_timestamp = 1640998861000
        end_timestamp = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=67
        )

        # Act
        query_params = result_engine.build_query_params(
            start_time=start_time, end_time=end_time
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_int(self):
        # Arrange
        start_time = 1640998861000
        end_time = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=67
        )

        # Act
        query_params = result_engine.build_query_params(
            start_time=start_time, end_time=end_time
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_time}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_time}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY
