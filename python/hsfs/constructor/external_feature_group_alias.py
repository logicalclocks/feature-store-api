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
from __future__ import annotations

from typing import Any, Dict, Union

import humps
from hsfs import feature_group


class ExternalFeatureGroupAlias:
    def __init__(
        self, on_demand_feature_group: Dict[str, Any], alias: str, **kwargs
    ) -> None:
        self._on_demand_feature_group: Union[
            feature_group.ExternalFeatureGroup, "feature_group.SpineGroup"
        ]
        if not on_demand_feature_group["spine"]:
            self._on_demand_feature_group = (
                feature_group.ExternalFeatureGroup.from_response_json(
                    on_demand_feature_group
                )
            )
        else:
            self._on_demand_feature_group = feature_group.SpineGroup.from_response_json(
                on_demand_feature_group
            )
        self._alias = alias

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> ExternalFeatureGroupAlias:
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def on_demand_feature_group(
        self,
    ) -> Union[feature_group.ExternalFeatureGroup, "feature_group.SpineGroup"]:
        return self._on_demand_feature_group

    @property
    def alias(self) -> str:
        return self._alias
