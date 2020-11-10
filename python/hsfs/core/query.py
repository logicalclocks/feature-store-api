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

import json

from hsfs import util, engine
from hsfs.core import join, query_constructor_api, storage_connector_api


class Query:
    def __init__(
        self,
        feature_store_name,
        feature_store_id,
        left_feature_group,
        left_features,
        left_featuregroup_start_time=None,
        left_featuregroup_end_time=None,
    ):
        self._feature_store_name = feature_store_name
        self._feature_store_id = feature_store_id
        self._left_feature_group = left_feature_group
        self._left_features = util.parse_features(left_features)
        self._left_featuregroup_start_time = left_featuregroup_start_time
        self._left_featuregroup_end_time = left_featuregroup_end_time
        self._joins = []
        self._query_constructor_api = query_constructor_api.QueryConstructorApi()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )

    def read(self, online=False, dataframe_type="default", read_options={}):
        query = self._query_constructor_api.construct_query(self)

        if online:
            sql_query = query.query_online
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            sql_query = query.query
            online_conn = None

            # Register on demand feature groups as temporary tables
            self._register_on_demand(query.on_demand_fg_aliases)

            # Register on hudi feature groups as temporary tables
            self._register_hudi_tables(
                query.hudi_cached_feature_groups,
                self._feature_store_id,
                self._feature_store_name,
                read_options,
            )

        return engine.get_instance().sql(
            sql_query, self._feature_store_name, online_conn, dataframe_type
        )

    def show(self, n, online=False):
        query = self._query_constructor_api.construct_query(self)

        if online:
            sql_query = query.query_online
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            sql_query = query.query
            online_conn = None

            # Register on demand feature groups as temporary tables
            self._register_on_demand(query.on_demand_fg_aliases)

            # Register on hudi feature groups as temporary tables
            self._register_hudi_tables(
                query.hudi_cached_feature_groups,
                self._feature_store_id,
                self._feature_store_name,
                {},
            )

        return engine.get_instance().show(
            sql_query, self._feature_store_name, n, online_conn
        )

    def join(self, sub_query, on=[], left_on=[], right_on=[], join_type="inner"):
        self._joins.append(
            join.Join(sub_query, on, left_on, right_on, join_type.upper())
        )
        return self

    def as_of(self, wallclock_time):
        for join in self._joins:
            join.query.left_featuregroup_end_time = wallclock_time
        self.left_featuregroup_end_time = wallclock_time
        return self

    def pull_changes(self, wallclock_start_time, wallclock_end_time):
        self.left_featuregroup_start_time = wallclock_start_time
        self.left_featuregroup_end_time = wallclock_end_time
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "leftFeatureGroup": self._left_feature_group,
            "leftFeatures": self._left_features,
            "leftFeatureGroupStartTime": self._left_featuregroup_start_time,
            "leftFeatureGroupEndTime": self._left_featuregroup_end_time,
            "joins": self._joins,
        }

    def to_string(self, online=False):
        fs_query_instance = self._query_constructor_api.construct_query(self)
        return fs_query_instance.query_online if online else fs_query_instance.query

    def __str__(self):
        return self._query_constructor_api.construct_query(self)

    def _register_on_demand(self, on_demand_fg_aliases):
        if on_demand_fg_aliases is None:
            return

        for on_demand_fg_alias in on_demand_fg_aliases:
            engine.get_instance().register_on_demand_temporary_table(
                on_demand_fg_alias.on_demand_feature_group.query,
                on_demand_fg_alias.on_demand_feature_group.storage_connector,
                on_demand_fg_alias.alias,
            )

    @property
    def left_featuregroup_start_time(self):
        return self._left_featuregroup_start_time

    @property
    def left_featuregroup_end_time(self):
        return self._left_featuregroup_start_time

    @left_featuregroup_start_time.setter
    def left_featuregroup_start_time(self, left_featuregroup_start_time):
        self._left_featuregroup_start_time = left_featuregroup_start_time

    @left_featuregroup_end_time.setter
    def left_featuregroup_end_time(self, left_featuregroup_start_time):
        self._left_featuregroup_end_time = left_featuregroup_start_time

    def _register_hudi_tables(
        self, hudi_feature_groups, feature_store_id, feature_store_name, read_options
    ):
        for hudi_fg in hudi_feature_groups:
            engine.get_instance().register_hudi_temporary_table(
                hudi_fg, feature_store_id, feature_store_name, read_options
            )
