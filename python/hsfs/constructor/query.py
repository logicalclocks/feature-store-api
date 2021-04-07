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
import humps
from typing import Optional, List, Union

from hsfs import util, engine
from hsfs.core import query_constructor_api, storage_connector_api
from hsfs.constructor import join, filter


class Query:
    def __init__(
        self,
        left_feature_group,
        left_features,
        feature_store_name=None,
        feature_store_id=None,
        left_feature_group_start_time=None,
        left_feature_group_end_time=None,
        joins=None,
        filter=None,
    ):
        self._feature_store_name = feature_store_name
        self._feature_store_id = feature_store_id
        self._left_feature_group = left_feature_group
        self._left_features = util.parse_features(left_features)
        self._left_feature_group_start_time = left_feature_group_start_time
        self._left_feature_group_end_time = left_feature_group_end_time
        self._joins = joins or []
        self._filter = filter
        self._hive_engine = True if engine.get_type() == "hive" else False
        self._query_constructor_api = query_constructor_api.QueryConstructorApi()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )

    def read(
        self,
        online: Optional[bool] = False,
        dataframe_type: Optional[str] = "default",
        read_options: Optional[dict] = {},
    ):
        """Read the specified query into a DataFrame.

        It is possible to specify the storage (online/offline) to read from and the
        type of the output DataFrame (Spark, Pandas, Numpy, Python Lists).

        # Arguments
            online: Read from online storage. Defaults to `False`.
            dataframe_type: DataFrame type to return. Defaults to `"default"`.
            read_options: Optional dictionary with read options for Spark.
                Defaults to `{}`.

        # Returns
            `DataFrame`: DataFrame depending on the chosen type.
        """
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

    def show(self, n: int, online: Optional[bool] = False):
        """Show the first N rows of the Query.

        # Arguments
            n: Number of rows to show.
            online: Show from online storage. Defaults to `False`.
        """
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

    def join(
        self,
        sub_query: "Query",
        on: Optional[List[str]] = [],
        left_on: Optional[List[str]] = [],
        right_on: Optional[List[str]] = [],
        join_type: Optional[str] = "inner",
    ):
        """Join Query with another Query.

        If no join keys are specified, Hopsworks will use the maximal matching subset of
        the primary keys of the feature groups you are joining.
        Joins of one level are supported, no neted joins.

        # Arguments
            sub_query: Right-hand side query to join.
            on: List of feature names to join on if they are available in both
                feature groups. Defaults to `[]`.
            left_on: List of feature names to join on from the left feature group of the
                join. Defaults to `[]`.
            right_on: List of feature names to join on from the right feature group of
                the join. Defaults to `[]`.
            join_type: Type of join to perform, can be `"inner"`, `"outer"`, `"left"` or
                `"right"`. Defaults to "inner".

        # Returns
            `Query`: A new Query object representing the join.
        """
        self._joins.append(
            join.Join(sub_query, on, left_on, right_on, join_type.upper())
        )
        return self

    def as_of(self, wallclock_time):
        wallclock_timestamp = util.get_timestamp_from_date_string(wallclock_time)
        for join in self._joins:
            join.query.left_feature_group_end_time = wallclock_timestamp
        self.left_feature_group_end_time = wallclock_timestamp
        return self

    def pull_changes(self, wallclock_start_time, wallclock_end_time):
        self.left_feature_group_start_time = util.get_timestamp_from_date_string(
            wallclock_start_time
        )
        self.left_feature_group_end_time = util.get_timestamp_from_date_string(
            wallclock_end_time
        )
        return self

    def filter(self, f: Union[filter.Filter, filter.Logic]):
        """Apply filter to the feature group.

        Selects all features and returns the resulting `Query` with the applied filter.

        ```python
        from hsfs.feature import Feature

        query.filter(Feature("weekly_sales") > 1000)
        ```

        If you are planning to join the filtered feature group later on with another
        feature group, make sure to select the filtered feature explicitly from the
        respective feature group:
        ```python
        query.filter(fg.feature1 == 1).show(10)
        ```

        Composite filters require parenthesis:
        ```python
        query.filter((fg.feature1 == 1) | (fg.feature2 >= 2))
        ```

        # Arguments
            f: Filter object.

        # Returns
            `Query`. The query object with the applied filter.
        """
        if self._filter is None:
            if isinstance(f, filter.Filter):
                self._filter = filter.Logic.Single(left_f=f)
            elif isinstance(f, filter.Logic):
                self._filter = f
            else:
                raise TypeError(
                    "Expected type `Filter` or `Logic`, got `{}`".format(type(f))
                )
        elif self._filter is not None:
            self._filter = self._filter & f
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "featureStoreName": self._feature_store_name,
            "featureStoreId": self._feature_store_id,
            "leftFeatureGroup": self._left_feature_group,
            "leftFeatures": self._left_features,
            "leftFeatureGroupStartTime": self._left_feature_group_start_time,
            "leftFeatureGroupEndTime": self._left_feature_group_end_time,
            "joins": self._joins,
            "filter": self._filter,
            "hiveEngine": self._hive_engine,
        }

    @classmethod
    def _hopsworks_json(cls, json_dict):
        """
        This method is used by the Hopsworks helper job.
        It does not fully deserialize the message as the usecase is to
        send it straight back to Hopsworks to read the content of the query

        Args:
            json_dict (str): a json string containing a query object

        Returns:
            A partially deserialize query object
        """
        json_decamelized = humps.decamelize(json_dict)
        new = cls(**json_decamelized)
        new._joins = humps.camelize(new._joins)
        return new

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
                on_demand_fg_alias.on_demand_feature_group,
                on_demand_fg_alias.alias,
            )

    @property
    def left_feature_group_start_time(self):
        return self._left_feature_group_start_time

    @property
    def left_feature_group_end_time(self):
        return self._left_feature_group_start_time

    @left_feature_group_start_time.setter
    def left_feature_group_start_time(self, left_feature_group_start_time):
        self._left_feature_group_start_time = left_feature_group_start_time

    @left_feature_group_end_time.setter
    def left_feature_group_end_time(self, left_feature_group_start_time):
        self._left_feature_group_end_time = left_feature_group_start_time

    def _register_hudi_tables(
        self, hudi_feature_groups, feature_store_id, feature_store_name, read_options
    ):
        for hudi_fg in hudi_feature_groups:
            engine.get_instance().register_hudi_temporary_table(
                hudi_fg, feature_store_id, feature_store_name, read_options
            )
