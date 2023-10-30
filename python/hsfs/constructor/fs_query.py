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
from hsfs import engine
from hsfs.constructor import hudi_feature_group_alias, external_feature_group_alias


class FsQuery:
    def __init__(
        self,
        query,
        on_demand_feature_groups,
        hudi_cached_feature_groups,
        query_online=None,
        pit_query=None,
        pit_query_asof=None,
        href=None,
        expand=None,
        items=None,
        type=None,
        **kwargs,
    ):
        self._query = query
        self._query_online = query_online
        self._pit_query = pit_query
        self._pit_query_asof = pit_query_asof

        if on_demand_feature_groups is not None:
            self._on_demand_fg_aliases = [
                external_feature_group_alias.ExternalFeatureGroupAlias.from_response_json(
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
    def pit_query(self):
        return self._pit_query

    @property
    def pit_query_asof(self):
        return self._pit_query_asof

    @property
    def on_demand_fg_aliases(self):
        return self._on_demand_fg_aliases

    @property
    def hudi_cached_feature_groups(self):
        return self._hudi_cached_feature_groups

    def register_external(self, spine=None):
        if self._on_demand_fg_aliases is None:
            return

        for external_fg_alias in self._on_demand_fg_aliases:
            if type(external_fg_alias.on_demand_feature_group).__name__ == "SpineGroup":
                external_fg_alias.on_demand_feature_group.dataframe = spine
            engine.get_instance().register_external_temporary_table(
                external_fg_alias.on_demand_feature_group,
                external_fg_alias.alias,
            )

    def register_hudi_tables(self, feature_store_id, feature_store_name, read_options):
        for hudi_fg in self._hudi_cached_feature_groups:
            engine.get_instance().register_hudi_temporary_table(
                hudi_fg, feature_store_id, feature_store_name, read_options
            )
