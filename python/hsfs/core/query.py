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
        self, feature_store_name, feature_store_id, left_feature_group, left_features
    ):
        self._feature_store_name = feature_store_name
        self._feature_store_id = feature_store_id
        self._left_feature_group = left_feature_group
        self._left_features = util.parse_features(left_features)
        self._joins = []
        self._query_constructor_api = query_constructor_api.QueryConstructorApi()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )

    def read(self, storage, dataframe_type="default"):
        query = self._query_constructor_api.construct_query(self)

        if storage == "online":
            sql_query = query["queryOnline"]
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            sql_query = query["query"]

        return engine.get_instance().sql(
            sql_query, self._feature_store_name, online_conn, dataframe_type
        )

    def show(self, n, storage):
        query = self._query_constructor_api.construct_query(self)["query"]

        if storage == "online":
            sql_query = query["queryOnline"]
            online_conn = self._storage_connector_api.get_online_connector()
        else:
            sql_query = query["query"]

        return engine.get_instance().show(
            sql_query, self._feature_store_name, n, online_conn
        )

    def join(self, sub_query, on=[], left_on=[], right_on=[], join_type="inner"):
        self._joins.append(
            join.Join(sub_query, on, left_on, right_on, join_type.upper())
        )
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "leftFeatureGroup": self._left_feature_group,
            "leftFeatures": self._left_features,
            "joins": self._joins,
        }

    def to_string(self):
        return self.__str__()

    def __str__(self):
        return self._query_constructor_api.construct_query(self)["query"]
