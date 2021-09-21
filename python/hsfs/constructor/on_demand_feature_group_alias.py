#
#   Copyright 2020 Logical Clocks AB
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

import humps

from hsfs import feature_group


class OnDemandFeatureGroupAlias:
    def __init__(self, on_demand_feature_group, alias):
        self._on_demand_feature_group = (
            feature_group.OnDemandFeatureGroup.from_response_json(
                on_demand_feature_group
            )
        )
        self._alias = alias

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def on_demand_feature_group(self):
        return self._on_demand_feature_group

    @property
    def alias(self):
        return self._alias
