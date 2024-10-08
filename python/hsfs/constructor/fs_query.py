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

from typing import Any, Dict, List, Optional, TypeVar, Union

import humps
from hsfs import engine
from hsfs.constructor import external_feature_group_alias, hudi_feature_group_alias


class FsQuery:
    def __init__(
        self,
        query: str,
        on_demand_feature_groups: Optional[List[Dict[str, Any]]],
        hudi_cached_feature_groups: Optional[List[Dict[str, Any]]],
        query_online: Optional[str] = None,
        pit_query: Optional[str] = None,
        pit_query_asof: Optional[str] = None,
        href: Optional[str] = None,
        expand: Optional[List[str]] = None,
        items: Optional[List[Dict[str, Any]]] = None,
        type: Optional[str] = None,
        delta_cached_feature_groups: Optional[List[Dict[str, Any]]] = None,
        **kwargs,
    ) -> None:
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

        if delta_cached_feature_groups is not None:
            self._delta_cached_feature_groups = [
                hudi_feature_group_alias.HudiFeatureGroupAlias.from_response_json(fg)
                for fg in delta_cached_feature_groups
            ]
        else:
            self._delta_cached_feature_groups = []

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> "FsQuery":
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def query(self) -> str:
        return self._query

    @property
    def query_online(self) -> Optional[str]:
        return self._query_online

    @property
    def pit_query(self) -> Optional[str]:
        return self._pit_query

    @property
    def pit_query_asof(self) -> Optional[str]:
        return self._pit_query_asof

    @property
    def on_demand_fg_aliases(
        self,
    ) -> List["external_feature_group_alias.ExternalFeatureGroupAlias"]:
        return self._on_demand_fg_aliases

    @property
    def hudi_cached_feature_groups(
        self,
    ) -> List["hudi_feature_group_alias.HudiFeatureGroupAlias"]:
        return self._hudi_cached_feature_groups

    def register_external(
        self,
        spine: Optional[
            Union[TypeVar("pyspark.sql.DataFrame"), TypeVar("pyspark.RDD")]
        ] = None,
    ) -> None:
        if self._on_demand_fg_aliases is None:
            return

        for external_fg_alias in self._on_demand_fg_aliases:
            if type(external_fg_alias.on_demand_feature_group).__name__ == "SpineGroup":
                external_fg_alias.on_demand_feature_group.dataframe = spine
            engine.get_instance().register_external_temporary_table(
                external_fg_alias.on_demand_feature_group,
                external_fg_alias.alias,
            )

    def register_hudi_tables(
        self,
        feature_store_id: int,
        feature_store_name: str,
        read_options: Optional[Dict[str, Any]],
    ) -> None:
        for hudi_fg in self._hudi_cached_feature_groups:
            engine.get_instance().register_hudi_temporary_table(
                hudi_fg, feature_store_id, feature_store_name, read_options
            )

    def register_delta_tables(
        self,
        feature_store_id: int,
        feature_store_name: str,
        read_options: Optional[Dict[str, Any]],
    ) -> None:
        for hudi_fg in self._delta_cached_feature_groups:
            engine.get_instance().register_delta_temporary_table(
                hudi_fg, feature_store_id, feature_store_name, read_options
            )
