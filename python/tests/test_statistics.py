#
#   Copyright 2022 Hopsworks AB
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


from hsfs import split_statistics, statistics
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class TestStatistics:
    def test_from_response_json_no_items(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_with_no_items"]["response"]

        # Act
        s = statistics.Statistics.from_response_json(json)

        # Assert
        assert s is None

    def test_from_response_json_no_filtered_items(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_with_no_filtered_items"]["response"]

        # Act
        s = statistics.Statistics.from_response_json(json)

        # Assert
        assert s is None

    def test_from_response_json_without_items_field(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_without_items_field"]["response"]

        stats = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        # Act
        s = statistics.Statistics.from_response_json(json)

        # Assert
        assert isinstance(s, statistics.Statistics)
        assert s.computation_time == "test_computation_time"
        assert s.window_end_commit_time == 11
        assert s.feature_descriptive_statistics is not None
        assert len(s.feature_descriptive_statistics) == 1
        assert isinstance(
            s.feature_descriptive_statistics[0], FeatureDescriptiveStatistics
        )
        assert s.feature_descriptive_statistics[0].id == stats.id
        assert s.split_statistics is None
        assert not s.before_transformation

    def test_from_response_json_single(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get"]["response"]

        stats = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        # Act
        s_lst = statistics.Statistics.from_response_json(json)

        # Assert
        assert isinstance(s_lst, list)
        assert len(s_lst) == 1
        s = s_lst[0]
        assert s.computation_time == "test_computation_time"
        assert s.window_end_commit_time == 11
        assert s.feature_descriptive_statistics is not None
        assert len(s.feature_descriptive_statistics) == 1
        assert isinstance(
            s.feature_descriptive_statistics[0], FeatureDescriptiveStatistics
        )
        assert s.feature_descriptive_statistics[0].id == stats.id
        assert s.split_statistics is None
        assert not s.before_transformation

    def test_from_response_json_multiple(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get"]["response"]

        stats = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        # Act
        s_lst = statistics.Statistics.from_response_json(json)

        # Assert
        assert isinstance(s_lst, list)
        assert len(s_lst) == 1
        s = s_lst[0]
        assert s.computation_time == "test_computation_time"
        assert s.window_end_commit_time == 11
        assert s.feature_descriptive_statistics is not None
        assert len(s.feature_descriptive_statistics) == 1
        assert isinstance(
            s.feature_descriptive_statistics[0], FeatureDescriptiveStatistics
        )
        assert s.feature_descriptive_statistics[0].id == stats.id
        assert s.split_statistics is None
        assert not s.before_transformation

    def test_from_response_json_with_split_statistics(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_with_split_statistics"]["response"]

        stats = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        # Act
        s_lst = statistics.Statistics.from_response_json(json)
        s = s_lst[0]

        # Assert
        assert s.computation_time == "test_computation_time"
        assert s.window_end_commit_time == 11
        assert s.feature_descriptive_statistics is None
        assert s.split_statistics is not None
        assert len(s.split_statistics) == 1
        assert isinstance(s.split_statistics[0], split_statistics.SplitStatistics)
        assert len(s.split_statistics[0].feature_descriptive_statistics) == 1
        assert isinstance(
            s.split_statistics[0].feature_descriptive_statistics[0],
            FeatureDescriptiveStatistics,
        )
        assert s.split_statistics[0].feature_descriptive_statistics[0].id == stats.id
        assert s.before_transformation is False

    def test_from_response_json_with_empty_data(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_with_empty_data"]["response"]

        stats = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"]["get_for_empty_data"][
                "response"
            ]
        )

        # Act
        s_lst = statistics.Statistics.from_response_json(json)
        s = s_lst[0]

        # Assert
        assert s.computation_time == "test_computation_time"
        assert s.window_end_commit_time is None
        assert s.feature_descriptive_statistics is not None
        assert len(s.feature_descriptive_statistics) == 1
        assert isinstance(
            s.feature_descriptive_statistics[0], FeatureDescriptiveStatistics
        )
        assert s.feature_descriptive_statistics[0].id == stats.id
        assert s.feature_descriptive_statistics[0].count == 0
        assert s.split_statistics is None
        assert s.before_transformation is False

    def test_from_response_json_before_transformation_functions(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_before_transformation_functions"][
            "response"
        ]

        stats = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_before_transformation_functions"
            ]["response"]
        )

        # Act
        s_lst = statistics.Statistics.from_response_json(json)
        s = s_lst[0]

        # Assert
        assert s.computation_time == "test_computation_time"
        assert s.window_end_commit_time is None
        assert s.feature_descriptive_statistics is not None
        assert len(s.feature_descriptive_statistics) == 1
        assert isinstance(
            s.feature_descriptive_statistics[0], FeatureDescriptiveStatistics
        )
        assert s.feature_descriptive_statistics[0].id == stats.id
        assert s.feature_descriptive_statistics[0].count is None
        assert s.feature_descriptive_statistics[0].extended_statistics is not None
        assert (
            "unique_values" in s.feature_descriptive_statistics[0].extended_statistics
        )
        assert s.split_statistics is None
        assert s.before_transformation is True

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_basic_info"]["response"]

        stats = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        # Act
        s_lst = statistics.Statistics.from_response_json(json)
        s = s_lst[0]

        # Assert
        assert s.computation_time == "test_computation_time"
        assert s.window_end_commit_time is None
        assert s.feature_descriptive_statistics is not None
        assert len(s.feature_descriptive_statistics) == 1
        assert isinstance(
            s.feature_descriptive_statistics[0], FeatureDescriptiveStatistics
        )
        assert s.feature_descriptive_statistics[0].id == stats.id
        assert s.split_statistics is None
        assert s.before_transformation is False

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_empty"]["response"]

        # Act
        s = statistics.Statistics.from_response_json(json)

        # Assert
        assert s is None
