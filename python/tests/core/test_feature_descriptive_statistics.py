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

from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult


class TestFeatureDescriptiveStatistics:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_descriptive_statistics"][
            "get_fractional_feature_statistics"
        ]["response"]

        # Act
        result = FeatureDescriptiveStatistics.from_response_json(result_json)

        # Assert
        assert isinstance(result, FeatureDescriptiveStatistics)
        assert result._id == 52
        assert result._feature_type == "Fractional"
        assert result._min == 1
        assert result._max == 2
        assert result._sum == 3
        assert result._count == 4
        assert result._mean == 5.1
        assert result._stddev == 6.1
        assert result._completeness == 1
        assert result._num_non_null_values == 7
        assert result._num_null_values == 8
        assert result._percentiles == {"25%": 0.4, "50%": 0.6, "75%": 0.86}
        assert result._approx_num_distinct_values == 9
        assert result._distinctness == 0.9
        assert result._entropy == 0.8
        assert result._uniqueness == 0.7
        assert result._exact_num_distinct_values == 10
        assert result._start_time == 123123123
        assert result._end_time == 123123124
        assert result._row_percentage == 100

    def test_from_deequ_json(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_descriptive_statistics"][
            "get_deequ_feature_statistics"
        ]["response"]

        # Act
        result = FeatureDescriptiveStatistics.from_deequ_json(result_json)

        # Assert
        assert isinstance(result, FeatureDescriptiveStatistics)
        assert result._feature_type == "Fractional"
        assert result._min == 1
        assert result._max == 2
        assert result._sum == 3
        assert result._count == 15
        assert result._mean == 5.1
        assert result._stddev == 6.1
        assert result._completeness == 1
        assert result._num_non_null_values == 7
        assert result._num_null_values == 8
        # assert result._percentiles == {"25%": 0.4, "50%": 0.6, "75%": 0.86}  # TODO: Parse deequ approxPercentiles
        assert result._approx_num_distinct_values == 9
        assert result._distinctness == 0.9
        assert result._entropy == 0.8
        assert result._uniqueness == 0.7
        assert result._exact_num_distinct_values == 10

    def test_from_response_json_via_fg_fm_result(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"][
            "get_via_feature_group"
        ]["response"]

        # Act
        result = FeatureMonitoringResult.from_response_json(result_json)

        # Assert
        assert isinstance(result, FeatureMonitoringResult)
        assert result._detection_stats_id is None
        assert result._reference_stats_id is None
        assert isinstance(result.detection_statistics, FeatureDescriptiveStatistics)
        assert isinstance(result.reference_statistics, FeatureDescriptiveStatistics)

        det_stats = result.detection_statistics
        assert det_stats._id == 52
        assert det_stats._feature_type == "Fractional"
        assert det_stats._min == 1
        assert det_stats._max == 2
        assert det_stats._sum == 3
        assert det_stats._count == 4
        assert det_stats._mean == 5.1
        assert det_stats._stddev == 6.1
        assert det_stats._completeness == 1
        assert det_stats._num_non_null_values == 7
        assert det_stats._num_null_values == 8
        # assert det_stats._percentiles == {"25%": 0.4, "50%": 0.6, "75%": 0.86}  # TODO: Parse deequ approxPercentiles
        assert det_stats._approx_num_distinct_values == 9
        assert det_stats._distinctness == 0.9
        assert det_stats._entropy == 0.8
        assert det_stats._uniqueness == 0.7
        assert det_stats._exact_num_distinct_values == 10
        assert det_stats._start_time == 123123123
        assert det_stats._end_time == 123123124
        assert det_stats._row_percentage == 100

        ref_stats = result.reference_statistics
        assert ref_stats._id == 53
        assert ref_stats._feature_type == "Fractional"
        assert ref_stats._min == 1
        assert ref_stats._max == 2
        assert ref_stats._sum == 3
        assert ref_stats._count == 4
        assert ref_stats._mean == 5.1
        assert ref_stats._stddev == 6.1
        assert ref_stats._completeness == 1
        assert ref_stats._num_non_null_values == 7
        assert ref_stats._num_null_values == 8
        # assert ref_stats._percentiles == {"25%": 0.4, "50%": 0.6, "75%": 0.86}  # TODO: Parse deequ approxPercentiles
        assert ref_stats._approx_num_distinct_values == 9
        assert ref_stats._distinctness == 0.9
        assert ref_stats._entropy == 0.8
        assert ref_stats._uniqueness == 0.7
        assert ref_stats._exact_num_distinct_values == 10
        assert ref_stats._start_time == 123123123
        assert ref_stats._end_time == 123123124
        assert ref_stats._row_percentage == 100

    def test_from_response_json_via_fv_fm_result(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"][
            "get_via_feature_view"
        ]["response"]

        # Act
        result = FeatureMonitoringResult.from_response_json(result_json)

        # Assert
        assert isinstance(result, FeatureMonitoringResult)
        assert result._detection_stats_id is None
        assert result._reference_stats_id is None
        assert isinstance(result.detection_statistics, FeatureDescriptiveStatistics)
        assert isinstance(result.reference_statistics, FeatureDescriptiveStatistics)

        det_stats = result.detection_statistics
        assert det_stats._id == 52
        assert det_stats._feature_type == "Fractional"
        assert det_stats._min == 1
        assert det_stats._max == 2
        assert det_stats._sum == 3
        assert det_stats._count == 4
        assert det_stats._mean == 5.1
        assert det_stats._stddev == 6.1
        assert det_stats._completeness == 1
        assert det_stats._num_non_null_values == 7
        assert det_stats._num_null_values == 8
        # assert det_stats._percentiles == {"25%": 0.4, "50%": 0.6, "75%": 0.86}  # TODO: Parse deequ approxPercentiles
        assert det_stats._approx_num_distinct_values == 9
        assert det_stats._distinctness == 0.9
        assert det_stats._entropy == 0.8
        assert det_stats._uniqueness == 0.7
        assert det_stats._exact_num_distinct_values == 10
        assert det_stats._start_time == 123123123
        assert det_stats._end_time == 123123124
        assert det_stats._row_percentage == 100

        ref_stats = result.reference_statistics
        assert ref_stats._id == 53
        assert ref_stats._feature_type == "Fractional"
        assert ref_stats._min == 1
        assert ref_stats._max == 2
        assert ref_stats._sum == 3
        assert ref_stats._count == 4
        assert ref_stats._mean == 5.1
        assert ref_stats._stddev == 6.1
        assert ref_stats._completeness == 1
        assert ref_stats._num_non_null_values == 7
        assert ref_stats._num_null_values == 8
        # assert ref_stats._percentiles == {"25%": 0.4, "50%": 0.6, "75%": 0.86}  # TODO: Parse deequ approxPercentiles
        assert ref_stats._approx_num_distinct_values == 9
        assert ref_stats._distinctness == 0.9
        assert ref_stats._entropy == 0.8
        assert ref_stats._uniqueness == 0.7
        assert ref_stats._exact_num_distinct_values == 10
        assert ref_stats._start_time == 123123123
        assert ref_stats._end_time == 123123124
        assert ref_stats._row_percentage == 100

    def test_from_response_json_via_fm_result_list_with_statistics(
        self, backend_fixtures
    ):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"][
            "get_list_with_statistics"
        ]["response"]

        # Act
        result_list = FeatureMonitoringResult.from_response_json(result_json)
        result = result_list[0]

        # Assert
        assert isinstance(result_list, list)
        assert len(result_list) == 1
        assert isinstance(result, FeatureMonitoringResult)
        assert result._detection_stats_id is None
        assert result._reference_stats_id is None
        assert isinstance(result.detection_statistics, FeatureDescriptiveStatistics)
        assert isinstance(result.reference_statistics, FeatureDescriptiveStatistics)

        det_stats = result.detection_statistics
        assert det_stats._id == 52
        assert det_stats._feature_type == "Fractional"
        assert det_stats._min == 1
        assert det_stats._max == 2
        assert det_stats._sum == 3
        assert det_stats._count == 4
        assert det_stats._mean == 5.1
        assert det_stats._stddev == 6.1
        assert det_stats._completeness == 1
        assert det_stats._num_non_null_values == 7
        assert det_stats._num_null_values == 8
        # assert det_stats._percentiles == {"25%": 0.4, "50%": 0.6, "75%": 0.86}  # TODO: Parse deequ approxPercentiles
        assert det_stats._approx_num_distinct_values == 9
        assert det_stats._distinctness == 0.9
        assert det_stats._entropy == 0.8
        assert det_stats._uniqueness == 0.7
        assert det_stats._exact_num_distinct_values == 10
        assert det_stats._start_time == 123123123
        assert det_stats._end_time == 123123124
        assert det_stats._row_percentage == 100

        ref_stats = result.reference_statistics
        assert ref_stats._id == 53
        assert ref_stats._feature_type == "Fractional"
        assert ref_stats._min == 1
        assert ref_stats._max == 2
        assert ref_stats._sum == 3
        assert ref_stats._count == 4
        assert ref_stats._mean == 5.1
        assert ref_stats._stddev == 6.1
        assert ref_stats._completeness == 1
        assert ref_stats._num_non_null_values == 7
        assert ref_stats._num_null_values == 8
        # assert ref_stats._percentiles == {"25%": 0.4, "50%": 0.6, "75%": 0.86}  # TODO: Parse deequ approxPercentiles
        assert ref_stats._approx_num_distinct_values == 9
        assert ref_stats._distinctness == 0.9
        assert ref_stats._entropy == 0.8
        assert ref_stats._uniqueness == 0.7
        assert ref_stats._exact_num_distinct_values == 10
        assert ref_stats._start_time == 123123123
        assert ref_stats._end_time == 123123124
        assert ref_stats._row_percentage == 100
