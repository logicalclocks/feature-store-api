#
#   Copyright 2022 Logical Clocks AB
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
import asyncio
import io
import logging
import re

import avro.io
import avro.schema
import numpy as np
import pandas as pd
from hsfs import client, feature_view, training_dataset, util
from hsfs.core import (
    feature_view_api,
    feature_view_engine,
    online_store_rest_client_engine,
    storage_connector_api,
    training_dataset_api,
    transformation_function_engine,
)
from hsfs.serving_key import ServingKey
from sqlalchemy import bindparam, exc, sql, text


_logger = logging.getLogger(__name__)


class VectorServer:
    DEFAULT_ONLINE_STORE_REST_CLIENT = "rest"
    DEFAULT_ONLINE_STORE_SQL_CLIENT = "sql"
    DEFAULT_ONLINE_STORE_CLIENT_KEY = "default_online_store_client"
    ONLINE_REST_CLIENT_CONFIG_OPTIONS_KEY = "config_online_store_rest_client"
    RESET_ONLINE_REST_CLIENT_OPTIONS_KEY = "reset_online_store_rest_client"

    def __init__(
        self,
        feature_store_id,
        features=None,
        training_dataset_version=None,
        serving_keys=None,
        skip_fg_ids=None,
        feature_store_name: str = None,
        feature_view_name: str = None,
        feature_view_version: int = None,
    ):
        if features is None:
            features = []
        self._training_dataset_version = training_dataset_version
        self._features = [] if features is None else features
        self._feature_vector_col_name = (
            [
                feat.name
                for feat in features
                if not (
                    feat.label
                    or feat.inference_helper_column
                    or feat.training_helper_column
                )
            ]
            if features
            else []
        )
        self._skip_fg_ids = skip_fg_ids or set()
        self._prepared_statement_engine = None
        self._prepared_statements = None
        self._helper_column_prepared_statements = None
        self._serving_keys = serving_keys
        self._valid_serving_key = None
        self._pkname_by_serving_index = None
        self._prefix_by_serving_index = None
        self._serving_key_by_serving_index = {}
        self._external = True
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            feature_store_id
        )
        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi()
        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                feature_store_id
            )
        )
        self._feature_view_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id
        )
        self._transformation_functions = None
        self._required_serving_keys = None
        self._async_pool = None

        self._online_store_rest_client_engine = None
        self._init_online_store_rest_client = None
        self._init_online_store_sql_client = None
        self._default_online_store_client = None

    def init_serving(
        self,
        entity: "feature_view.FeatureView",
        batch: bool,
        external: bool = None,
        inference_helper_columns: bool = False,
        init_online_store_sql_client: bool = True,
        init_online_store_rest_client: bool = False,
        options=None,
    ):
        self._set_default_online_store_client(
            init_online_store_rest_client=init_online_store_rest_client,
            init_online_store_sql_client=init_online_store_sql_client,
            options=options,
        )

        if external is None:
            external = isinstance(client.get_instance(), client.external.Client)
        # `init_prepared_statement` should be the last because other initialisations
        # has to be done successfully before it is able to fetch feature vectors.
        self.init_transformation(entity)
        self._complex_features = self.get_complex_feature_schemas()

        if self._init_online_store_rest_client is True:
            _logger.info("Initialising Vector Server Online Store REST client")
            self._online_store_rest_client_engine = online_store_rest_client_engine.OnlineStoreRestClientEngine(
                feature_store_name=self._feature_store_name,
                feature_view_name=entity.name,
                feature_view_version=entity.version,
                features=entity.features,
                skip_fg_ids=self._skip_fg_ids,
                # Code duplication added to avoid unnecessary transforming and iterating over feature vectors
                # multiple times. This is a temporary solution until the code is refactored with new sql client
                complex_features=self._complex_features,
                transformation_functions=self._transformation_functions,
                # Temporary fix to allow for missing primary keys in the entry
                serving_keys=self._serving_keys,
            )
            reset_online_rest_client = False
            online_store_rest_client_config = None
            if isinstance(options, dict):
                reset_online_rest_client = options.get(
                    self.RESET_ONLINE_REST_CLIENT_OPTIONS_KEY, reset_online_rest_client
                )
                online_store_rest_client_config = options.get(
                    self.ONLINE_REST_CLIENT_CONFIG_OPTIONS_KEY,
                    online_store_rest_client_config,
                )

            client.online_store_rest_client.init_or_reset_online_store_rest_client(
                optional_config=online_store_rest_client_config,
                reset_client=reset_online_rest_client,
            )

        if self._init_online_store_sql_client is True:
            _logger.info("Initialising Vector Server Online SQL client")
            self.init_prepared_statement(
                entity,
                batch,
                external,
                inference_helper_columns,
                options=options,
            )

    def init_batch_scoring(self, entity):
        self.init_transformation(entity)

    def init_transformation(self, entity):
        # attach transformation functions
        self._transformation_functions = self._get_transformation_fns(entity)

    def init_prepared_statement(
        self,
        entity,
        batch,
        external,
        inference_helper_columns,
        options=None,
    ):
        if isinstance(entity, feature_view.FeatureView):
            if inference_helper_columns:
                helper_column_prepared_statements = (
                    self._feature_view_api.get_serving_prepared_statement(
                        entity.name, entity.version, batch, inference_helper_columns
                    )
                )
            prepared_statements = self._feature_view_api.get_serving_prepared_statement(
                entity.name, entity.version, batch, inference_helper_columns=False
            )
        elif isinstance(entity, training_dataset.TrainingDataset):
            prepared_statements = (
                self._training_dataset_api.get_serving_prepared_statement(entity, batch)
            )
        else:
            raise ValueError(
                "Object type needs to be `feature_view.FeatureView` or `training_dataset.TrainingDataset`."
            )
        # reset values to default, as user may be re-initialising with different parameters
        self._prepared_statement_engine = None
        self._prepared_statements = None
        self._helper_column_prepared_statements = None
        self._external = external
        # create aiomysql engine
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            self._set_aiomysql_connection(len(prepared_statements), options=options)
        )

        (
            self._prepared_statements,
            feature_name_order_by_psp,
            prefix_by_serving_index,
        ) = self._parametrize_prepared_statements(prepared_statements, batch)

        if inference_helper_columns:
            (
                self._helper_column_prepared_statements,
                _,
                _,
            ) = self._parametrize_prepared_statements(
                helper_column_prepared_statements, batch
            )

        self._prefix_by_serving_index = prefix_by_serving_index
        for sk in self._serving_keys:
            self._serving_key_by_serving_index[
                sk.join_index
            ] = self._serving_key_by_serving_index.get(sk.join_index, []) + [sk]
        # sort the serving by PreparedStatementParameter.index
        for join_index in self._serving_key_by_serving_index:
            # feature_name_order_by_psp do not include the join index when the joint feature only contains label only
            # But _serving_key_by_serving_index include the index when the join_index is 0 (left side)
            if join_index in feature_name_order_by_psp:
                self._serving_key_by_serving_index[join_index] = sorted(
                    self._serving_key_by_serving_index[join_index],
                    key=lambda _sk: feature_name_order_by_psp[join_index].get(
                        _sk.feature_name, 0
                    ),
                )

        self._valid_serving_key = set(
            [sk.feature_name for sk in self._serving_keys]
            + [sk.required_serving_key for sk in self._serving_keys]
        )

    def _parametrize_prepared_statements(self, prepared_statements, batch):
        prepared_statements_dict = {}
        serving_keys = set()
        feature_name_order_by_psp = dict()
        prefix_by_serving_index = {}
        for prepared_statement in prepared_statements:
            if prepared_statement.feature_group_id in self._skip_fg_ids:
                continue
            query_online = str(prepared_statement.query_online).replace("\n", " ")
            prefix_by_serving_index[
                prepared_statement.prepared_statement_index
            ] = prepared_statement.prefix

            # In java prepared statement `?` is used for parametrization.
            # In sqlalchemy `:feature_name` is used instead of `?`
            pk_names = [
                psp.name
                for psp in sorted(
                    prepared_statement.prepared_statement_parameters,
                    key=lambda psp: psp.index,
                )
            ]
            feature_name_order_by_psp[
                prepared_statement.prepared_statement_index
            ] = dict(
                [
                    (psp.name, psp.index)
                    for psp in prepared_statement.prepared_statement_parameters
                ]
            )
            # construct serving key if it is not provided.
            if self._serving_keys is None:
                for pk_name in pk_names:
                    # should use `ignore_prefix=True` because before hsfs 3.3,
                    # only primary key of a feature group are matched in
                    # user provided entry.
                    serving_key = ServingKey(
                        feature_name=pk_name,
                        join_index=prepared_statement.prepared_statement_index,
                        prefix=prepared_statement.prefix,
                        ignore_prefix=True,
                    )
                    serving_keys.add(serving_key)

            if not batch:
                for pk_name in pk_names:
                    query_online = self._parametrize_query(pk_name, query_online)
                query_online = sql.text(query_online)
            else:
                query_online = self._parametrize_query("batch_ids", query_online)
                query_online = sql.text(query_online)
                query_online = query_online.bindparams(
                    batch_ids=bindparam("batch_ids", expanding=True)
                )

            prepared_statements_dict[
                prepared_statement.prepared_statement_index
            ] = query_online

        # assign serving key if it is not provided.
        if self._serving_keys is None:
            self._serving_keys = serving_keys

        return (
            prepared_statements_dict,
            feature_name_order_by_psp,
            prefix_by_serving_index,
        )

    def _validate_serving_key(self, entry):
        for key in entry:
            if key not in self._valid_serving_key:
                raise ValueError(
                    f"'{key}' is not a correct serving key. Expect one of the"
                    f" followings: [{', '.join(self._valid_serving_key)}]"
                )

    def get_feature_vector(
        self,
        entry,
        return_type=None,
        passed_features=None,
        allow_missing=False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
    ):
        """Assembles serving vector from online feature store."""
        if passed_features is None:
            passed_features = []

        online_client_str = self.which_client_and_ensure_initialised(
            force_rest_client=force_rest_client, force_sql_client=force_sql_client
        )

        if online_client_str == self.DEFAULT_ONLINE_STORE_REST_CLIENT:
            _logger.info("get_feature_vector Online REST client")
            vector = self._online_store_rest_client_engine.get_single_feature_vector(
                entry=entry,
                passed_features=passed_features,
                return_type=self._online_store_rest_client_engine.RETURN_TYPE_FEATURE_VALUE_LIST,
            )

        else:  # aiomysql branch
            # get result row
            _logger.info("get_feature_vector Online SQL client")
            serving_vector = self._vector_result(entry, self._prepared_statements)
            # Add the passed features
            serving_vector.update(passed_features)

            # apply transformation functions
            result_dict = self._apply_transformation(serving_vector)

            vector = self._generate_vector(result_dict, allow_missing)

        if return_type.lower() == "list":
            return vector
        elif return_type.lower() == "numpy":
            return np.array(vector)
        elif return_type.lower() == "pandas":
            pandas_df = pd.DataFrame(vector).transpose()
            pandas_df.columns = self._feature_vector_col_name
            return pandas_df
        else:
            raise ValueError(
                "Unknown return type. Supported return types are 'list','pandas' and 'numpy'"
            )

    def get_feature_vectors(
        self,
        entries,
        return_type=None,
        passed_features=None,
        allow_missing=False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
    ):
        if passed_features is None:
            passed_features = []
        """Assembles serving vector from online feature store."""
        online_client_str = self.which_client_and_ensure_initialised(
            force_rest_client=force_rest_client, force_sql_client=force_sql_client
        )

        if online_client_str == self.DEFAULT_ONLINE_STORE_REST_CLIENT:
            _logger.info("get_feature_vectors through REST client")
            vectors = self._online_store_rest_client_engine.get_batch_feature_vectors(
                entries=entries,
                passed_features=passed_features,
                return_type=self._online_store_rest_client_engine.RETURN_TYPE_FEATURE_VALUE_LIST,
            )
        else:
            _logger.info("get_feature_vectors through SQL client")
            batch_results, _ = self._batch_vector_results(
                entries, self._prepared_statements
            )
            # apply passed features to each batch result
            for vector_index, pf in enumerate(passed_features):
                batch_results[vector_index].update(pf)

            # apply transformation functions
            batch_transformed = list(
                map(
                    lambda results_dict: self._apply_transformation(results_dict),
                    batch_results,
                )
            )

            # get vectors
            vectors = []
            for result in batch_transformed:
                # for backward compatibility, before 3.4, if result is empty,
                # instead of throwing error, it skips the result
                if len(result) != 0 or allow_missing:
                    vectors.append(self._generate_vector(result, fill_na=allow_missing))

        if return_type.lower() == "list":
            return vectors
        elif return_type.lower() == "numpy":
            return np.array(vectors)
        elif return_type.lower() == "pandas":
            pandas_df = pd.DataFrame(vectors)
            pandas_df.columns = self._feature_vector_col_name
            return pandas_df
        else:
            raise ValueError(
                "Unknown return type. Supported return types are 'list', 'pandas' and 'numpy'"
            )

    def which_client_and_ensure_initialised(
        self, force_rest_client: bool, force_sql_client: bool
    ) -> str:
        """Check if the requested client is initialised as well as deciding which client to use based on default.

        # Arguments:
            force_rest_client: bool. user specified override to use rest_client.
            force_sql_client: bool. user specified override to use sql_client.

        # Returns:
            An enum specifying the client to be used.
        """
        if force_rest_client and force_sql_client:
            raise ValueError(
                "force_rest_client and force_sql_client cannot be used at the same time."
            )

        # No override, use default client
        if not force_rest_client and not force_sql_client:
            return self.default_online_store_client

        if (
            self._init_online_store_rest_client is False
            and self._init_online_store_sql_client is False
        ):
            raise ValueError(
                "No client is initialised. Call `init_serving` with init_online_store_sql_client or init_online_store_rest_client set to True before using it."
            )
        if force_sql_client and (self._init_online_store_sql_client is False):
            raise ValueError(
                "SQL Client is not initialised. Call `init_serving` with init_online_store_sql_client set to True before using it."
            )
        elif force_sql_client:
            return self.DEFAULT_ONLINE_STORE_SQL_CLIENT

        if force_rest_client and (self._init_online_store_rest_client is False):
            raise ValueError(
                "RonDB Rest Client is not initialised. Call `init_serving` with init_online_store_rest_client set to True before using it."
            )
        elif force_rest_client:
            return self.DEFAULT_ONLINE_STORE_REST_CLIENT

    def _set_default_online_store_client(
        self,
        init_online_store_rest_client: bool,
        init_online_store_sql_client: bool,
        options: dict,
    ):
        if (
            init_online_store_rest_client is False
            and init_online_store_sql_client is False
        ):
            raise ValueError(
                "At least one of the clients should be initialised. Set init_online_store_sql_client or init_online_store_rest_client to True."
            )
        self._init_online_store_rest_client = init_online_store_rest_client
        self._init_online_store_sql_client = init_online_store_sql_client

        if (
            init_online_store_rest_client is True
            and init_online_store_sql_client is True
        ):
            # Defaults to SQL as client for legacy reasons mainly.
            self.default_online_store_client = (
                options.get(
                    "default_online_store_client", self.DEFAULT_ONLINE_STORE_SQL_CLIENT
                )
                if isinstance(options, dict)
                else self.DEFAULT_ONLINE_STORE_SQL_CLIENT
            )
        elif init_online_store_rest_client is True:
            self.default_online_store_client = self.DEFAULT_ONLINE_STORE_REST_CLIENT
        else:
            self.default_online_store_client = self.DEFAULT_ONLINE_STORE_SQL_CLIENT

    def get_inference_helper(self, entry, return_type):
        """Assembles serving vector from online feature store."""

        serving_vector = self._vector_result(
            entry, self._helper_column_prepared_statements
        )

        if return_type.lower() == "pandas":
            return pd.DataFrame([serving_vector])
        elif return_type.lower() == "dict":
            return serving_vector
        else:
            raise ValueError(
                "Unknown return type. Supported return types are 'pandas' and 'dict'"
            )

    def get_inference_helpers(
        self,
        feature_view_object,
        entries,
        return_type,
    ):
        """Assembles serving vector from online feature store."""
        batch_results, serving_keys = self._batch_vector_results(
            entries, self._helper_column_prepared_statements
        )

        # drop serving and primary key names from the result dict
        drop_list = serving_keys + list(feature_view_object.primary_keys)
        _ = list(
            map(
                lambda results_dict: [
                    results_dict.pop(x, None)
                    for x in drop_list
                    if x not in feature_view_object.inference_helper_columns
                ],
                batch_results,
            )
        )

        if return_type.lower() == "dict":
            return batch_results
        elif return_type.lower() == "pandas":
            return pd.DataFrame(batch_results)
        else:
            raise ValueError(
                "Unknown return type. Supported return types are 'dict' and 'pandas'"
            )

    def _vector_result(self, entry, prepared_statement_objects):
        """Retrieve single vector with parallel queries using aiomysql engine."""

        if all([isinstance(val, list) for val in entry.values()]):
            raise ValueError(
                "Entry is expected to be single value per primary key. "
                "If you have already initialised prepared statements for single vector and now want to retrieve "
                "batch vector please reinitialise prepared statements with  "
                "`training_dataset.init_prepared_statement()` "
                "or `feature_view.init_serving()`"
            )
        self._validate_serving_key(entry)
        # Initialize the set of values
        serving_vector = {}
        bind_entries = {}
        prepared_statement_execution = {}
        for prepared_statement_index in prepared_statement_objects:
            pk_entry = {}
            next_statement = False
            for sk in self._serving_key_by_serving_index[prepared_statement_index]:
                if sk.required_serving_key not in entry.keys():
                    # Check if there is any entry matched with feature name.
                    if sk.feature_name in entry.keys():
                        pk_entry[sk.feature_name] = entry[sk.feature_name]
                    else:
                        # User did not provide the necessary serving keys, we expect they have
                        # provided the necessary features as passed_features.
                        # We are going to check later if this is true
                        next_statement = True
                        break
                else:
                    pk_entry[sk.feature_name] = entry[sk.required_serving_key]
            if next_statement:
                continue
            bind_entries[prepared_statement_index] = pk_entry
            prepared_statement_execution[
                prepared_statement_index
            ] = prepared_statement_objects[prepared_statement_index]

        # run all the prepared statements in parallel using aiomysql engine
        loop = asyncio.get_event_loop()
        results_dict = loop.run_until_complete(
            self._execute_prep_statements(prepared_statement_execution, bind_entries)
        )

        for key in results_dict:
            for row in results_dict[key]:
                result_dict = self.deserialize_complex_features(dict(row))
                serving_vector.update(result_dict)

        return serving_vector

    def _batch_vector_results(self, entries, prepared_statement_objects):
        """Execute prepared statements in parallel using aiomysql engine."""

        # create dict object that will have of order of the vector as key and values as
        # vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
        # expect that backend will return correctly ordered vectors.
        batch_results = [{} for _ in range(len(entries))]
        entry_values = {}
        serving_keys_all_fg = []
        prepared_stmts_to_execute = {}
        # construct the list of entry values for binding to query
        for prepared_statement_index in prepared_statement_objects:
            # prepared_statement_index include fg with label only
            # But _serving_key_by_serving_index include the index when the join_index is 0 (left side)
            if prepared_statement_index not in self._serving_key_by_serving_index:
                continue

            prepared_stmts_to_execute[
                prepared_statement_index
            ] = prepared_statement_objects[prepared_statement_index]
            entry_values_tuples = list(
                map(
                    lambda e: tuple(
                        [
                            (
                                e.get(sk.required_serving_key)
                                # Check if there is any entry matched with feature name,
                                # if the required serving key is not provided.
                                or e.get(sk.feature_name)
                            )
                            for sk in self._serving_key_by_serving_index[
                                prepared_statement_index
                            ]
                        ]
                    ),
                    entries,
                )
            )
            entry_values[prepared_statement_index] = {"batch_ids": entry_values_tuples}

        # run all the prepared statements in parallel using aiomysql engine
        loop = asyncio.get_event_loop()
        parallel_results = loop.run_until_complete(
            self._execute_prep_statements(prepared_stmts_to_execute, entry_values)
        )

        # construct the results
        for prepared_statement_index in prepared_stmts_to_execute:
            statement_results = {}
            serving_keys = self._serving_key_by_serving_index[prepared_statement_index]
            serving_keys_all_fg += serving_keys
            # Use prefix from prepare statement because prefix from serving key is collision adjusted.
            prefix_features = [
                (self._prefix_by_serving_index[prepared_statement_index] or "")
                + sk.feature_name
                for sk in self._serving_key_by_serving_index[prepared_statement_index]
            ]
            # iterate over results by index of the prepared statement
            for row in parallel_results[prepared_statement_index]:
                row_dict = dict(row)
                # can primary key be complex feature?
                result_dict = self.deserialize_complex_features(row_dict)
                # note: should used serialized value
                # as it is from users' input
                statement_results[
                    self._get_result_key(prefix_features, row_dict)
                ] = result_dict

            # add partial results to the global results
            for i, entry in enumerate(entries):
                batch_results[i].update(
                    statement_results.get(
                        self._get_result_key_serving_key(serving_keys, entry), {}
                    )
                )
        return batch_results, serving_keys_all_fg

    def get_complex_feature_schemas(self):
        return {
            f.name: avro.io.DatumReader(
                avro.schema.parse(
                    f._feature_group._get_feature_avro_schema(
                        f.feature_group_feature_name
                    )
                )
            )
            for f in self._features
            if f.is_complex()
        }

    def deserialize_complex_features(self, row_dict):
        for feature_name, schema in self._complex_features.items():
            if feature_name in row_dict:
                bytes_reader = io.BytesIO(row_dict[feature_name])
                decoder = avro.io.BinaryDecoder(bytes_reader)
                row_dict[feature_name] = schema.read(decoder)
        return row_dict

    def refresh_mysql_connection(self):
        try:
            with self._prepared_statement_engine.connect():
                pass
        except exc.OperationalError:
            self._set_mysql_connection()

    def _make_preview_statement(self, statement, n):
        return text(statement.text[: statement.text.find(" WHERE ")] + f" LIMIT {n}")

    def _set_mysql_connection(self, options=None):
        online_conn = self._storage_connector_api.get_online_connector(
            self._feature_store_id
        )
        self._prepared_statement_engine = util.create_mysql_engine(
            online_conn, self._external, options=options
        )

    def _generate_vector(self, result_dict, fill_na=False):
        # feature values
        vector = []
        for feature_name in self._feature_vector_col_name:
            if feature_name not in result_dict:
                if fill_na:
                    vector.append(None)
                else:
                    raise client.exceptions.FeatureStoreException(
                        f"Feature '{feature_name}' is missing from vector."
                        "Possible reasons: "
                        "1. There is no match in the given entry."
                        " Please check if the entry exists in the online feature store"
                        " or provide the feature as passed_feature. "
                        f"2. Required entries [{', '.join(self.required_serving_keys)}] or "
                        f"[{', '.join(set(sk.feature_name for sk in self._serving_keys))}] are not provided."
                    )
            else:
                vector.append(result_dict[feature_name])
        return vector

    def _apply_transformation(self, row_dict):
        for feature_name in self._transformation_functions:
            if feature_name in row_dict:
                transformation_fn = self._transformation_functions[
                    feature_name
                ].transformation_fn
                row_dict[feature_name] = transformation_fn(row_dict[feature_name])
        return row_dict

    @staticmethod
    def _get_result_key(primary_keys, result_dict):
        result_key = []
        for pk in primary_keys:
            result_key.append(result_dict.get(pk))
        return tuple(result_key)

    @staticmethod
    def _get_result_key_serving_key(serving_keys, result_dict):
        result_key = []
        for sk in serving_keys:
            result_key.append(
                result_dict.get(sk.required_serving_key)
                or result_dict.get(sk.feature_name)
            )
        return tuple(result_key)

    @staticmethod
    def _parametrize_query(name, query_online):
        # Now we have ordered pk_names, iterate over it and replace `?` with `:feature_name` one by one.
        # Regex `"^(.*?)\?"` will identify 1st occurrence of `?` in the sql string and replace it with provided
        # feature name, e.g. `:feature_name`:. As we iteratively update `query_online` we are always aiming to
        # replace 1st occurrence of `?`. This approach can only work if primary key names are sorted properly.
        # Regex `"^(.*?)\?"`:
        # `^` - asserts position at start of a line
        # `.*?` - matches any character (except for line terminators). `*?` Quantifier —
        # Matches between zero and unlimited times, expanding until needed, i.e 1st occurrence of `\?`
        # character.
        return re.sub(
            r"^(.*?)\?",
            r"\1:" + name,
            query_online,
        )

    def _get_transformation_fns(self, entity):
        # get attached transformation functions
        transformation_functions = (
            self._transformation_function_engine.get_td_transformation_fn(entity)
            if isinstance(entity, training_dataset.TrainingDataset)
            else (
                self._feature_view_engine.get_attached_transformation_fn(
                    entity.name, entity.version
                )
            )
        )
        is_stat_required = (
            len(
                set(self._transformation_function_engine.BUILTIN_FN_NAMES).intersection(
                    set([tf.name for tf in transformation_functions.values()])
                )
            )
            > 0
        )
        is_feat_view = isinstance(entity, feature_view.FeatureView)
        if not is_stat_required:
            td_tffn_stats = None
        else:
            # if there are any built-in transformation functions get related statistics and
            # populate with relevant arguments
            # there should be only one statistics object with before_transformation=true
            if is_feat_view and self._training_dataset_version is None:
                raise ValueError(
                    "Training data version is required for transformation. Call `feature_view.init_serving(version)` "
                    "or `feature_view.init_batch_scoring(version)` to pass the training dataset version."
                    "Training data can be created by `feature_view.create_training_data` or `feature_view.get_training_data`."
                )
            td_tffn_stats = self._feature_view_engine._statistics_engine.get(
                entity,
                before_transformation=True,
                training_dataset_version=self._training_dataset_version,
            )

        if is_stat_required and td_tffn_stats is None:
            raise ValueError(
                "No statistics available for initializing transformation functions."
            )

        transformation_fns = (
            self._transformation_function_engine.populate_builtin_attached_fns(
                transformation_functions,
                td_tffn_stats.feature_descriptive_statistics
                if td_tffn_stats is not None
                else None,
            )
        )
        return transformation_fns

    def filter_entry_by_join_index(self, entry, join_index):
        fg_entry = {}
        complete = True
        for sk in self._serving_key_by_serving_index[join_index]:
            fg_entry[sk.feature_name] = entry.get(sk.required_serving_key) or entry.get(
                sk.feature_name
            )  # fallback to use raw feature name
            if fg_entry[sk.feature_name] is None:
                complete = False
                break
        return complete, fg_entry

    async def _set_aiomysql_connection(self, default_min_size: int, options=None):
        online_connector = self._storage_connector_api.get_online_connector(
            self._feature_store_id
        )
        self._async_pool = await util.create_async_engine(
            online_connector, self._external, default_min_size, options=options
        )

    async def _query_async_sql(self, stmt, bind_params):
        """Query prepared statement together with bind params using aiomysql connection pool"""
        # Get a connection from the pool
        async with self._async_pool.acquire() as conn:
            # Execute the prepared statement
            cursor = await conn.execute(stmt, bind_params)
            # Fetch the result
            resultset = await cursor.fetchall()
            await cursor.close()

        return resultset

    async def _execute_prep_statements(self, prepared_statements: dict, entries: dict):
        """Iterate over prepared statements to create async tasks
        and gather all tasks results for a given list of entries."""

        # validate if prepared_statements and entries have the same keys
        if prepared_statements.keys() != entries.keys():
            # iterate over prepared_statements and entries to find the missing key
            # remove missing keys from prepared_statements
            for key in list(prepared_statements.keys()):
                if key not in entries:
                    prepared_statements.pop(key)

        tasks = [
            asyncio.ensure_future(
                self._query_async_sql(prepared_statements[key], entries[key])
            )
            for key in prepared_statements
        ]
        # Run the queries in parallel using asyncio.gather
        results = await asyncio.gather(*tasks)
        results_dict = {}
        # Create a dict of results with the prepared statement index as key
        for i, key in enumerate(prepared_statements):
            results_dict[key] = results[i]
        # TODO: close connection? closing connection pool here will make un-acquirable for future connections
        # unless init_serving is explicitly called before get_feature_vector(s).
        return results_dict

    @property
    def prepared_statement_engine(self):
        """JDBC connection engine to retrieve connections to online features store from."""
        return self._prepared_statement_engine

    @prepared_statement_engine.setter
    def prepared_statement_engine(self, prepared_statement_engine):
        self._prepared_statement_engine = prepared_statement_engine

    @property
    def prepared_statements(self):
        """The dict object of prepared_statements as values and kes as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(self, prepared_statements):
        self._prepared_statements = prepared_statements

    @property
    def required_serving_keys(self):
        """Set of primary key names that is used as keys in input dict object for `get_feature_vector` method."""
        if self._required_serving_keys is not None:
            return self._required_serving_keys
        if self._serving_keys is not None:
            self._required_serving_keys = set(
                [key.required_serving_key for key in self._serving_keys]
            )
        else:
            self._required_serving_keys = set()
        return self._required_serving_keys

    @property
    def serving_keys(self):
        return self._serving_keys

    @serving_keys.setter
    def serving_keys(self, serving_vector_keys):
        self._serving_keys = serving_vector_keys

    @property
    def training_dataset_version(self):
        return self._training_dataset_version

    @training_dataset_version.setter
    def training_dataset_version(self, training_dataset_version):
        self._training_dataset_version = training_dataset_version

    @property
    def default_online_store_client(self) -> str:
        return self._default_online_store_client

    @default_online_store_client.setter
    def default_online_store_client(self, default_online_store_client: str):
        if default_online_store_client not in [
            self.DEFAULT_ONLINE_STORE_REST_CLIENT,
            self.DEFAULT_ONLINE_STORE_SQL_CLIENT,
        ]:
            raise ValueError(
                f"Default Online Feature Store Client should be one of {self.DEFAULT_ONLINE_STORE_REST_CLIENT} or {self.DEFAULT_ONLINE_STORE_SQL_CLIENT}."
            )

        if (
            default_online_store_client == self.DEFAULT_ONLINE_STORE_REST_CLIENT
            and self._init_online_store_rest_client is False
        ):
            raise ValueError(
                f"Default Online Store cCient is set to {self.DEFAULT_ONLINE_STORE_REST_CLIENT} but Online Store REST client"
                + " is not initialised. Call `init_serving` with init_client set to True before using it."
            )
        elif (
            default_online_store_client == self.DEFAULT_ONLINE_STORE_SQL_CLIENT
            and self._init_online_store_sql_client is False
        ):
            raise ValueError(
                f"Default online client is set to {self.DEFAULT_ONLINE_STORE_SQL_CLIENT} but Online Store SQL client"
                + " is not initialised. Call `init_serving` with init_online_store_sql_client set to True before using it."
            )

        _logger.info(
            f"Default Online Store Client is set to {default_online_store_client}."
        )
        self._default_online_store_client = default_online_store_client
