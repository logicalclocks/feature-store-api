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
from hsfs.core.time_travel_format import TimeTravelFormat


class Query:
    def __init__(
        self,
        feature_store_name,
        feature_store_id,
        left_feature_group,
        left_features,
        left_featuregroup_starttime=None,
        left_featuregroup_endtime=None,
    ):
        self._feature_store_name = feature_store_name
        self._feature_store_id = feature_store_id
        self._left_feature_group = left_feature_group
        self._left_features = util.parse_features(left_features)
        self._left_featuregroup_starttime = left_featuregroup_starttime
        self._left_featuregroup_endtime = left_featuregroup_endtime
        self._joins = []
        self._query_constructor_api = query_constructor_api.QueryConstructorApi()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )

    def read(
        self,
        storage="offline",
        dataframe_type="default",
        time_travel_fomat=TimeTravelFormat.HUDI,
    ):
        query = self._query_constructor_api.construct_query(self)

        if storage.lower() == "online":
            sql_query = query.query_online
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            sql_query = query.query
            # Register on demand feature groups as temporary tables
            self._register_on_demand(query.on_demand_fg_aliases)
            online_conn = None
            # Register on hudi feature groups as temporary tables
            if time_travel_fomat == TimeTravelFormat.HUDI:
                self._register_hudi_tables(query.hudi_fg_aliases)

        return engine.get_instance().sql(
            sql_query, self._feature_store_name, online_conn, dataframe_type
        )

    def show(self, n, storage="offline"):
        query = self._query_constructor_api.construct_query(self)

        if storage.lower() == "online":
            sql_query = query.query_online
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            sql_query = query.query
            # Register on demand feature groups as temporary tables
            self._register_on_demand(query.on_demand_fg_aliases)
            online_conn = None

        return engine.get_instance().show(
            sql_query, self._feature_store_name, n, online_conn
        )

    def join(self, sub_query, on=[], left_on=[], right_on=[], join_type="inner"):
        self._joins.append(
            join.Join(sub_query, on, left_on, right_on, join_type.upper())
        )
        return self

    def as_of(self, wallclocktime):
        for join in self._joins:
            join.query.left_featuregroup_endtime = wallclocktime
        self.left_featuregroup_endtime = wallclocktime
        return self

    def pull_changes(self, wallclock_starttime, wallclock_endtime):
        self.left_featuregroup_starttime = wallclock_starttime
        self.left_featuregroup_endtime = wallclock_endtime
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "leftFeatureGroup": self._left_feature_group,
            "leftFeatures": self._left_features,
            "leftFeaturegroupStartTime": self._left_featuregroup_starttime,
            "leftFeaturegroupEndTime": self._left_featuregroup_endtime,
            "joins": self._joins,
        }

    def to_string(self, storage="offline"):
        query = self._query_constructor_api.construct_query(self)
        return query.query if storage == "offline" else query.query_online

    def __str__(self):
        return self._query_constructor_api.construct_query(self)

    def _register_on_demand(self, on_demand_fg_aliases):
        if on_demand_fg_aliases is None:
            return

        for on_demand_fg_alias in on_demand_fg_aliases:
            engine.get_instance().register_temporary_table(
                on_demand_fg_alias.on_demand_feature_group.query,
                on_demand_fg_alias.on_demand_feature_group.storage_connector,
                on_demand_fg_alias.alias,
            )

    def _register_hudi_tables(self, hudi_fg_aliases):

        # TODO (check that its spark engine otherwise throw exception):
        for hudi_fg_alias in hudi_fg_aliases:
            engine.get_instance().register_hudi_temporary_table(hudi_fg_alias)

    @property
    def left_featuregroup_starttime(self):
        return self._left_featuregroup_starttime

    @property
    def left_featuregroup_endtime(self):
        return self._left_featuregroup_starttime

    @left_featuregroup_starttime.setter
    def left_featuregroup_starttime(self, left_featuregroup_starttime):
        self._left_featuregroup_starttime = left_featuregroup_starttime

    @left_featuregroup_endtime.setter
    def left_featuregroup_endtime(self, left_featuregroup_starttime):
        self._left_featuregroup_endtime = left_featuregroup_starttime
