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


from hsfs import statistics, split_statistics


class TestStatistics:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get"]["response"]

        # Act
        s = statistics.Statistics.from_response_json(json)

        # Assert
        assert s.commit_time == "test_commit_time"
        assert s.feature_group_commit_id == 11
        assert s.content == {"key": "value"}
        assert len(s.split_statistics) == 1
        assert isinstance(s.split_statistics[0], split_statistics.SplitStatistics)
        assert s.for_transformation == True

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_basic_info"]["response"]

        # Act
        s = statistics.Statistics.from_response_json(json)

        # Assert
        assert s.commit_time == "test_commit_time"
        assert s.feature_group_commit_id == None
        assert s.content == {}
        assert len(s.split_statistics) == 0
        assert s.for_transformation == False

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["statistics"]["get_empty"]["response"]

        # Act
        s = statistics.Statistics.from_response_json(json)

        # Assert
        assert s == None
