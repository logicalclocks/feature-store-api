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

from hsfs.core.feature_monitoring_result_engine import FeatureMonitoringResultEngine
from datetime import date
import dateutil

DEFAULT_MONITORING_TIME_SORT_BY = "monitoring_time:desc"


class TestFeatureMonitoringResultEngine:
    def test_build_query_params_none(self):
        # Arrange
        start_time = None
        end_time = None

        result_engine = FeatureMonitoringResultEngine(feature_store_id=67)

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

        result_engine = FeatureMonitoringResultEngine(feature_store_id=67)

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

        result_engine = FeatureMonitoringResultEngine(feature_store_id=67)

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

        result_engine = FeatureMonitoringResultEngine(feature_store_id=67)

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

        result_engine = FeatureMonitoringResultEngine(feature_store_id=67)

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
