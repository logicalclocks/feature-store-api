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
from hsfs.constructor import external_feature_group_alias


class TestExternalFeatureGroupAlias:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["external_feature_group_alias"]["get"]["response"]

        # Act
        external_fg_alias = (
            external_feature_group_alias.ExternalFeatureGroupAlias.from_response_json(
                json
            )
        )

        # Assert
        assert isinstance(
            external_fg_alias.on_demand_feature_group,
            feature_group.ExternalFeatureGroup,
        )
        assert external_fg_alias.alias == "test_alias"
