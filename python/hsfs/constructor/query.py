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
from datetime import datetime, date

from hsfs import util, engine, feature_group
from hsfs.core import query_constructor_api, storage_connector_api, arrow_flight_client
from hsfs.constructor import join
from hsfs.constructor.filter import Filter, Logic
from hsfs.client.exceptions import FeatureStoreException
from hsfs.feature import Feature


class Query:
    ERROR_MESSAGE_FEATURE_AMBIGUOUS = (
        "Provided feature name '{}' is ambiguous and exists in more than one feature group. "
        "Consider prepending the prefix specified in the join."
    )
    ERROR_MESSAGE_FEATURE_AMBIGUOUS_FG = (
        "Feature name '{}' is ambiguous and exists in more than one feature group. "
        "Consider accessing the feature through the feature group object when specifying the query."
    )
    ERROR_MESSAGE_FEATURE_NOT_FOUND = (
        "Feature name '{}' could not found be found in query."
    )
    ERROR_MESSAGE_FEATURE_NOT_FOUND_FG = (
        "Feature name '{}' could not be found in "
        "any of the featuregroups in this query."
    )

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
        self._filter = Logic.from_response_json(filter)
        self._python_engine = True if engine.get_type() == "python" else False
        self._query_constructor_api = query_constructor_api.QueryConstructorApi()
        self._storage_connector_api = storage_connector_api.StorageConnectorApi()

    def _prep_read(self, online, read_options):
        fs_query = self._query_constructor_api.construct_query(self)

        if online:
            sql_query = self._to_string(fs_query, online)
            online_conn = self._storage_connector_api.get_online_connector(
                self._feature_store_id
            )
        else:
            online_conn = None

            if engine.get_instance().is_flyingduck_query_supported(self, read_options):
                sql_query = self._to_string(fs_query, online, asof=True)
                sql_query = arrow_flight_client.get_instance().create_query_object(
                    self, sql_query, fs_query.on_demand_fg_aliases
                )
            else:
                sql_query = self._to_string(fs_query, online)
                # Register on demand feature groups as temporary tables
                if isinstance(self._left_feature_group, feature_group.SpineGroup):
                    fs_query.register_external(self._left_feature_group.dataframe)
                else:
                    fs_query.register_external()

                # Register on hudi feature groups as temporary tables
                fs_query.register_hudi_tables(
                    self._feature_store_id,
                    self._feature_store_name,
                    read_options,
                )

        return sql_query, online_conn

    def read(
        self,
        online: Optional[bool] = False,
        dataframe_type: Optional[str] = "default",
        read_options: Optional[dict] = {},
    ):
        """Read the specified query into a DataFrame.

        It is possible to specify the storage (online/offline) to read from and the
        type of the output DataFrame (Spark, Pandas, Numpy, Python Lists).

        !!! warning "External Feature Group Engine Support"
            **Spark only**

            Reading a Query containing an External Feature Group directly into a
            Pandas Dataframe using Python/Pandas as Engine is not supported,
            however, you can use the Query API to create Feature Views/Training
            Data containing External Feature Groups.

        # Arguments
            online: Read from online storage. Defaults to `False`.
            dataframe_type: DataFrame type to return. Defaults to `"default"`.
            read_options: Dictionary of read options for Spark in spark engine.
                Only for python engine:
                * key `"use_hive"` and value `True` to read query with Hive instead of
                  [ArrowFlight Server](https://docs.hopsworks.ai/latest/setup_installation/common/arrow_flight_duckdb/).
                * key "hive_config" to pass a dictionary of hive or tez configurations.
                  For example: `{"hive_config": {"hive.tez.cpu.vcores": 2, "tez.grouping.split-count": "3"}}`
                Defaults to `{}`.

        # Returns
            `DataFrame`: DataFrame depending on the chosen type.
        """
        if not read_options:
            read_options = {}

        sql_query, online_conn = self._prep_read(online, read_options)

        schema = None
        if (
            read_options
            and "pandas_types" in read_options
            and read_options["pandas_types"]
        ):
            schema = self.features
            if len(self.joins) > 0 or None in [f.type for f in schema]:
                raise ValueError(
                    "Pandas types casting only supported for feature_group.read()/query.select_all()"
                )

        return engine.get_instance().sql(
            sql_query,
            self._feature_store_name,
            online_conn,
            dataframe_type,
            read_options,
            schema,
        )

    def show(self, n: int, online: Optional[bool] = False):
        """Show the first N rows of the Query.

        !!! example "Show the first 10 rows"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")

            query = fg1.select_all().join(fg2.select_all())

            query.show(10)
            ```

        # Arguments
            n: Number of rows to show.
            online: Show from online storage. Defaults to `False`.
        """
        read_options = {}
        sql_query, online_conn = self._prep_read(online, read_options)

        return engine.get_instance().show(
            sql_query, self._feature_store_name, n, online_conn, read_options
        )

    def join(
        self,
        sub_query: "Query",
        on: Optional[List[str]] = [],
        left_on: Optional[List[str]] = [],
        right_on: Optional[List[str]] = [],
        join_type: Optional[str] = "inner",
        prefix: Optional[str] = None,
    ):
        """Join Query with another Query.

        If no join keys are specified, Hopsworks will use the maximal matching subset of
        the primary keys of the feature groups you are joining.
        Joins of one level are supported, no nested joins.

        !!! example "Join two feature groups"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")

            query = fg1.select_all().join(fg2.select_all())
            ```

        !!! example "More complex join"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")
            fg3 = fs.get_feature_group("...")

            query = fg1.select_all()
                    .join(fg2.select_all(), on=["date", "location_id"])
                    .join(fg3.select_all(), left_on=["location_id"], right_on=["id"], how="left")
            ```

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
            prefix: User provided prefix to avoid feature name clash. Prefix is applied to the right
                feature group of the query. Defaults to `None`.

        # Returns
            `Query`: A new Query object representing the join.
        """
        self._joins.append(
            join.Join(sub_query, on, left_on, right_on, join_type.upper(), prefix)
        )

        return self

    def as_of(
        self,
        wallclock_time: Optional[Union[str, int, datetime, date]] = None,
        exclude_until: Optional[Union[str, int, datetime, date]] = None,
    ):
        """Perform time travel on the given Query.

        This method returns a new Query object at the specified point in time. Optionally, commits before a
        specified point in time can be excluded from the query. The Query can then either be read into a Dataframe
        or used further to perform joins or construct a training dataset.

        !!! example "Reading features at a specific point in time:"
            ```python
            fs = connection.get_feature_store();
            query = fs.get_feature_group("example_feature_group", 1).select_all()
            query.as_of("2020-10-20 07:34:11").read().show()
            ```

        !!! example "Reading commits incrementally between specified points in time:"
            ```python
            fs = connection.get_feature_store();
            query = fs.get_feature_group("example_feature_group", 1).select_all()
            query.as_of("2020-10-20 07:34:11", exclude_until="2020-10-19 07:34:11").read().show()
            ```

        The first parameter is inclusive while the latter is exclusive.
        That means, in order to query a single commit, you need to query that commit time
        and exclude everything just before the commit.

        !!! example "Reading only the changes from a single commit"
            ```python
            fs = connection.get_feature_store();
            query = fs.get_feature_group("example_feature_group", 1).select_all()
            query.as_of("2020-10-20 07:31:38", exclude_until="2020-10-20 07:31:37").read().show()
            ```

        When no wallclock_time is given, the latest state of features is returned. Optionally, commits before
        a specified point in time can still be excluded.

        !!! example "Reading the latest state of features, excluding commits before a specified point in time"
            ```python
            fs = connection.get_feature_store();
            query = fs.get_feature_group("example_feature_group", 1).select_all()
            query.as_of(None, exclude_until="2020-10-20 07:31:38").read().show()
            ```

        Note that the interval will be applied to all joins in the query.
        If you want to query different intervals for different feature groups in
        the query, you have to apply them in a nested fashion:
        ```python
        query1.as_of(..., ...)
            .join(query2.as_of(..., ...))
        ```

        If instead you apply another `as_of` selection after the join, all
        joined feature groups will be queried with this interval:
        ```python
        query1.as_of(..., ...)  # as_of is not applied
            .join(query2.as_of(..., ...))  # as_of is not applied
            .as_of(..., ...)
        ```

        !!! warning
            This function only works for queries on feature groups with time_travel_format='HUDI'.

        !!! warning
            Excluding commits via exclude_until is only possible within the range of the Hudi active timeline.
            By default, Hudi keeps the last 20 to 30 commits in the active timeline.
            If you need to keep a longer active timeline, you can overwrite the options:
            `hoodie.keep.min.commits` and `hoodie.keep.max.commits`
            when calling the `insert()` method.

        # Arguments
            wallclock_time: Read data as of this point in time.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, or `%Y-%m-%d %H:%M:%S`.
            exclude_until: Exclude commits until this point in time. Strings should be formatted in one of the
                following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, or `%Y-%m-%d %H:%M:%S`.

        # Returns
            `Query`. The query object with the applied time travel condition.
        """
        wallclock_timestamp = util.convert_event_time_to_timestamp(wallclock_time)

        exclude_until_timestamp = util.convert_event_time_to_timestamp(exclude_until)

        for _join in self._joins:
            _join.query.left_feature_group_end_time = wallclock_timestamp
            _join.query.left_feature_group_start_time = exclude_until_timestamp
        self.left_feature_group_end_time = wallclock_timestamp
        self.left_feature_group_start_time = exclude_until_timestamp
        return self

    def pull_changes(self, wallclock_start_time, wallclock_end_time):
        """
        !!! warning "Deprecated"
        `pull_changes` method is deprecated. Use
        `as_of(end_wallclock_time, exclude_until=start_wallclock_time) instead.
        """
        self.left_feature_group_start_time = util.convert_event_time_to_timestamp(
            wallclock_start_time
        )
        self.left_feature_group_end_time = util.convert_event_time_to_timestamp(
            wallclock_end_time
        )
        return self

    def filter(self, f: Union[Filter, Logic]):
        """Apply filter to the feature group.

        Selects all features and returns the resulting `Query` with the applied filter.
        !!! example
            ```python

            from hsfs.feature import Feature

            query.filter(Feature("weekly_sales") > 1000)
            query.filter(Feature("name").like("max%"))

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

        !!! example "Filters are fully compatible with joins"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")
            fg3 = fs.get_feature_group("...")

            query = fg1.select_all()
                .join(fg2.select_all(), on=["date", "location_id"])
                .join(fg3.select_all(), left_on=["location_id"], right_on=["id"], how="left")
                .filter((fg1.location_id == 10) | (fg1.location_id == 20))
            ```

        !!! example "Filters can be applied at any point of the query"
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")
            fg3 = fs.get_feature_group("...")

            query = fg1.select_all()
                .join(fg2.select_all().filter(fg2.avg_temp >= 22), on=["date", "location_id"])
                .join(fg3.select_all(), left_on=["location_id"], right_on=["id"], how="left")
                .filter(fg1.location_id == 10)
            ```

        # Arguments
            f: Filter object.

        # Returns
            `Query`. The query object with the applied filter.
        """
        if self._filter is None:
            if isinstance(f, Filter):
                self._filter = Logic.Single(left_f=f)
            elif isinstance(f, Logic):
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
            "hiveEngine": self._python_engine,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        feature_group_json = json_decamelized["left_feature_group"]
        if (
            feature_group_json["type"] == "onDemandFeaturegroupDTO"
            and not feature_group_json["spine"]
        ):
            feature_group_obj = feature_group.ExternalFeatureGroup.from_response_json(
                feature_group_json
            )
        elif (
            feature_group_json["type"] == "onDemandFeaturegroupDTO"
            and feature_group_json["spine"]
        ):
            feature_group_obj = feature_group.SpineGroup.from_response_json(
                feature_group_json
            )
        else:
            feature_group_obj = feature_group.FeatureGroup.from_response_json(
                feature_group_json
            )
        return cls(
            left_feature_group=feature_group_obj,
            left_features=json_decamelized["left_features"],
            feature_store_name=json_decamelized.get("feature_store_name", None),
            feature_store_id=json_decamelized.get("feature_store_id", None),
            left_feature_group_start_time=json_decamelized.get(
                "left_feature_group_start_time", None
            ),
            left_feature_group_end_time=json_decamelized.get(
                "left_feature_group_end_time", None
            ),
            joins=[
                join.Join.from_response_json(_join)
                for _join in json_decamelized.get("joins", [])
            ],
            filter=json_decamelized.get("filter", None),
        )

    @classmethod
    def _hopsworks_json(cls, json_dict):
        """
        This method is used by the Hopsworks helper job.
        It does not fully deserialize the message as the usecase is to
        send it straight back to Hopsworks to read the content of the query

        Arguments:
            json_dict (str): a json string containing a query object

        Returns:
            A partially deserialize query object
        """
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("hive_engine", None)
        new = cls(**json_decamelized)
        new._joins = humps.camelize(new._joins)
        return new

    def to_string(self, online=False, arrow_flight=False):
        """
        !!! example
            ```python
            fg1 = fs.get_feature_group("...")
            fg2 = fs.get_feature_group("...")

            query = fg1.select_all().join(fg2.select_all())

            query.to_string()
            ```
        """
        fs_query = self._query_constructor_api.construct_query(self)

        return self._to_string(fs_query, online, arrow_flight)

    def _to_string(self, fs_query, online=False, asof=False):
        if online:
            return fs_query.query_online
        if fs_query.pit_query is not None:
            if asof:
                return fs_query.pit_query_asof
            else:
                return fs_query.pit_query
        return fs_query.query

    def __str__(self):
        return self._query_constructor_api.construct_query(self)

    @property
    def left_feature_group_start_time(self):
        """Start time of time travel for the left feature group."""
        return self._left_feature_group_start_time

    @property
    def left_feature_group_end_time(self):
        """End time of time travel for the left feature group."""
        return self._left_feature_group_end_time

    @left_feature_group_start_time.setter
    def left_feature_group_start_time(self, left_feature_group_start_time):
        self._left_feature_group_start_time = left_feature_group_start_time

    @left_feature_group_end_time.setter
    def left_feature_group_end_time(self, left_feature_group_end_time):
        self._left_feature_group_end_time = left_feature_group_end_time

    def append_feature(self, feature):
        """
        Append a feature to the query.

        # Arguments
            feature: `[str, Feature]`. Name of the feature to append to the query.
        """
        feature = util.validate_feature(feature)

        self._left_features.append(feature)

        return self

    def is_time_travel(self):
        """Query contains time travel"""
        return (
            self.left_feature_group_start_time
            or self.left_feature_group_end_time
            or any([_join.query.is_time_travel() for _join in self._joins])
        )

    def is_cache_feature_group_only(self):
        """Query contains only cached feature groups"""
        return all(
            [isinstance(fg, feature_group.FeatureGroup) for fg in self.featuregroups]
        )

    def _get_featuregroup_by_feature(self, feature: Feature):
        # search for feature by id, and return the fg object
        fg_id = feature._feature_group_id
        for fg in self.featuregroups:
            if fg.id == fg_id:
                return fg

        # search for feature by name and collect fg objects
        featuregroup_features = {}
        for feat in self._left_feature_group.features:
            featuregroup_features[feat.name] = featuregroup_features.get(
                feat.name, []
            ) + [self._left_feature_group]
        for join_obj in self.joins:
            for feat in join_obj.query._left_feature_group.features:
                featuregroup_features[feat.name] = featuregroup_features.get(
                    feat.name, []
                ) + [join_obj.query._left_feature_group]
        featuregroups_found = featuregroup_features.get(feature.name, [])

        if len(featuregroups_found) > 1:
            raise FeatureStoreException(
                Query.ERROR_MESSAGE_FEATURE_AMBIGUOUS_FG.format(feature.name)
            )
        elif len(featuregroups_found) == 1:
            return featuregroups_found[0]

        raise FeatureStoreException(
            Query.ERROR_MESSAGE_FEATURE_NOT_FOUND_FG.format(feature.name)
        )

    def _get_feature_by_name(
        self,
        feature_name: str,
    ):
        # collect a dict that maps feature names -> (feature, prefix, fg)
        query_features = {}
        for feat in self._left_features:
            feature_entry = (feat, None, self._left_feature_group)
            query_features[feat.name] = query_features.get(feat.name, []) + [
                feature_entry
            ]
        for join_obj in self.joins:
            for feat in join_obj.query._left_features:
                feature_entry = (
                    feat,
                    join_obj.prefix,
                    join_obj.query._left_feature_group,
                )
                query_features[feat.name] = query_features.get(feat.name, []) + [
                    feature_entry
                ]
                # if the join has a prefix, add a lookup for "prefix.feature_name"
                if join_obj.prefix:
                    name_with_prefix = f"{join_obj.prefix}{feat.name}"
                    query_features[name_with_prefix] = query_features.get(
                        name_with_prefix, []
                    ) + [feature_entry]

        if feature_name not in query_features:
            raise FeatureStoreException(
                Query.ERROR_MESSAGE_FEATURE_NOT_FOUND.format(feature_name)
            )

        # return (feature, prefix, fg) tuple, if only one match was found
        feats = query_features[feature_name]
        if len(feats) == 1:
            return feats[0]

        # if multiple matches were found, return the one without prefix
        feats_without_prefix = [feat for feat in feats if feat[1] is None]
        if len(feats_without_prefix) == 1:
            return feats_without_prefix[0]

        # there were multiple ambiguous matches
        raise FeatureStoreException(
            Query.ERROR_MESSAGE_FEATURE_AMBIGUOUS.format(feature_name)
        )

    @property
    def joins(self):
        """List of joins in the query"""
        return self._joins

    @property
    def featuregroups(self):
        """List of feature groups used in the query"""
        featuregroups = {self._left_feature_group}
        for join_obj in self.joins:
            featuregroups.add(join_obj.query._left_feature_group)
        return list(featuregroups)

    @property
    def filters(self):
        """All filters used in the query"""
        filters = self._filter
        for join_obj in self.joins:
            if filters is None:
                filters = join_obj.query._filter
            elif join_obj.query._filter is not None:
                filters = filters & join_obj.query._filter

        return filters

    @property
    def features(self):
        """List of all features in the query"""
        features = []
        for feat in self._left_features:
            features.append(feat)

        for join_obj in self.joins:
            for feat in join_obj.query._left_features:
                features.append(feat)

        return features

    def get_feature(self, feature_name):
        """
        Get a feature by name.

        # Arguments
            feature_name: `str`. Name of the feature to get.

        # Returns
            `Feature`. Feature object.
        """
        return self._get_feature_by_name(feature_name)[0]

    def __getattr__(self, name):
        try:
            return self.__getitem__(name)
        except FeatureStoreException:
            raise AttributeError(f"'Query' object has no attribute '{name}'. ")

    def __getitem__(self, name):
        if not isinstance(name, str):
            raise TypeError(
                f"Expected type `str`, got `{type(name)}`. "
                "Features are accessible by name."
            )
        return self.get_feature(name)
