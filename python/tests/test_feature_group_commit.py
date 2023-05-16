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


from hsfs import feature_group_commit


class TestFeatureGroupCommit:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group_commit"]["get_list"]["response"]

        # Act
        fg_commit = feature_group_commit.FeatureGroupCommit.from_response_json(json)[0]

        # Assert
        assert fg_commit.commitid == 11
        assert fg_commit.commit_date_string == "test_commit_date_string"
        assert fg_commit.commit_time == "test_commit_time"
        assert fg_commit.rows_inserted == 1
        assert fg_commit.rows_updated == 2
        assert fg_commit.rows_deleted == 3
        assert fg_commit.validation_id == 77

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group_commit"]["get_list"]["response"]

        # Act
        fg_commit_list = feature_group_commit.FeatureGroupCommit.from_response_json(
            json
        )

        # Assert
        assert len(fg_commit_list) == 1
        fg_commit = fg_commit_list[0]
        assert fg_commit.commitid == 11
        assert fg_commit.commit_date_string == "test_commit_date_string"
        assert fg_commit.commit_time == "test_commit_time"
        assert fg_commit.rows_inserted == 1
        assert fg_commit.rows_updated == 2
        assert fg_commit.rows_deleted == 3
        assert fg_commit.validation_id == 77

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group_commit"]["get_basic_info"]["response"]

        # Act
        fg_commit = feature_group_commit.FeatureGroupCommit.from_response_json(json)

        # Assert
        assert len(fg_commit) == 0
