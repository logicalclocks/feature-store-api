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


from hsfs import feature_group
from hsfs.constructor import hudi_feature_group_alias


class TestFsQuery:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["hudi_feature_group_alias"]["get"]["response"]

        # Act
        hudi_fg_alias = (
            hudi_feature_group_alias.HudiFeatureGroupAlias.from_response_json(json)
        )

        # Assert
        assert isinstance(hudi_fg_alias._feature_group, feature_group.FeatureGroup)
        assert hudi_fg_alias.alias == "test_alias"
        assert (
            hudi_fg_alias._left_feature_group_start_timestamp == "test_start_timestamp"
        )
        assert hudi_fg_alias._left_feature_group_end_timestamp == "test_end_timestamp"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["hudi_feature_group_alias"]["get_basic_info"][
            "response"
        ]

        # Act
        hudi_fg_alias = (
            hudi_feature_group_alias.HudiFeatureGroupAlias.from_response_json(json)
        )

        # Assert
        assert isinstance(hudi_fg_alias._feature_group, feature_group.FeatureGroup)
        assert hudi_fg_alias.alias == "test_alias"
        assert hudi_fg_alias._left_feature_group_start_timestamp is None
        assert hudi_fg_alias._left_feature_group_end_timestamp is None
