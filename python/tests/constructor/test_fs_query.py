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


from hsfs.constructor import (
    external_feature_group_alias,
    fs_query,
    hudi_feature_group_alias,
)


class TestFsQuery:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["fs_query"]["get"]["response"]

        # Act
        q = fs_query.FsQuery.from_response_json(json)

        # Assert
        assert q.query == "test_query"
        assert len(q._on_demand_fg_aliases) == 1
        assert isinstance(
            q._on_demand_fg_aliases[0],
            external_feature_group_alias.ExternalFeatureGroupAlias,
        )
        assert len(q._hudi_cached_feature_groups) == 1
        assert isinstance(
            q._hudi_cached_feature_groups[0],
            hudi_feature_group_alias.HudiFeatureGroupAlias,
        )
        assert q.query_online == "test_query_online"
        assert q.pit_query == "test_pit_query"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["fs_query"]["get_basic_info"]["response"]

        # Act
        q = fs_query.FsQuery.from_response_json(json)

        # Assert
        assert q.query == "test_query"
        assert len(q._on_demand_fg_aliases) == 0
        assert len(q._hudi_cached_feature_groups) == 0
        assert q.query_online is None
        assert q.pit_query is None
