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
from hsfs.core import hudi_feature_group_alias, on_demand_feature_group_alias


class FsQuery:
    def __init__(
        self,
        query,
        query_online,
        on_demand_feature_groups,
        hudi_cached_feature_groups,
        href=None,
        expand=None,
        items=None,
        type=None,
    ):
        self._query = query
        self._query_online = query_online

        if on_demand_feature_groups is not None:
            self._on_demand_fg_aliases = [
                on_demand_feature_group_alias.OnDemandFeatureGroupAlias.from_response_json(
                    fg
                )
                for fg in on_demand_feature_groups
            ]
        else:
            self._on_demand_fg_aliases = []

        if hudi_cached_feature_groups is not None:
            self._hudi_cached_feature_groups = [
                hudi_feature_group_alias.HudiFeatureGroupAlias.from_response_json(fg)
                for fg in hudi_cached_feature_groups
            ]
        else:
            self._hudi_cached_feature_groups = []

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def query(self):
        return self._query

    @property
    def query_online(self):
        return self._query_online

    @property
    def on_demand_fg_aliases(self):
        return self._on_demand_fg_aliases

    @property
    def hudi_cached_feature_groups(self):
        return self._hudi_cached_feature_groups
