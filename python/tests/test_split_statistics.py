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


from hsfs import split_statistics
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class TestSplitStatistics:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["split_statistics"]["get"]["response"]

        statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        # Act
        ss = split_statistics.SplitStatistics.from_response_json(json)

        # Assert
        assert ss.name == "test_name"
        assert len(ss.feature_descriptive_statistics) == 1
        assert isinstance(
            ss.feature_descriptive_statistics[0], FeatureDescriptiveStatistics
        )
        assert ss.feature_descriptive_statistics[0].id == statistics.id

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["split_statistics"]["get_basic_info"]["response"]

        statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )

        # Act
        ss = split_statistics.SplitStatistics.from_response_json(json)

        # Assert
        assert ss.name == "test_name"
        assert len(ss.feature_descriptive_statistics) == 1
        assert isinstance(
            ss.feature_descriptive_statistics[0], FeatureDescriptiveStatistics
        )
        assert ss.feature_descriptive_statistics[0].id == statistics.id
