#
#   Copyright 2024 Hopsworks AB
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

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import aiomysql
import aiomysql.utils
from hsfs import feature_view, storage_connector, training_dataset, util
from hsfs.constructor.serving_prepared_statement import ServingPreparedStatement
from hsfs.core import feature_view_api, storage_connector_api, training_dataset_api
from hsfs.serving_key import ServingKey
from sqlalchemy import bindparam, exc, sql, text


_logger = logging.getLogger(__name__)


class OnlineStoreSqlClient:
    BATCH_HELPER_KEY = "batch_helper_column"
    SINGLE_HELPER_KEY = "single_helper_column"
    BATCH_VECTOR_KEY = "batch_feature_vectors"
    SINGLE_VECTOR_KEY = "single_feature_vector"

    def __init__(
        self,
        feature_store_id: id,
        skip_fg_ids: Optional[Set[int]],
        external: bool,
        serving_keys: Optional[Set[ServingKey]] = None,
        connection_options: Optional[Dict[str, Any]] = None,
    ):
        _logger.debug("Initialising Online Store Sql Client")
        self._feature_store_id = feature_store_id
        self._skip_fg_ids: Set[int] = skip_fg_ids or set()
        self._external = external

        self._prefix_by_serving_index = None
        self._pkname_by_serving_index = None
        self._serving_key_by_serving_index: Dict[str, ServingKey] = {}
        self._connection_pool = None
        self._serving_keys: Set[ServingKey] = set(serving_keys or [])

        self._prepared_statements: Dict[str, List[ServingPreparedStatement]] = {}
        self._parametrised_prepared_statements = {}
        self._prepared_statement_engine = None

        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            feature_store_id
        )
        self._storage_connector_api = storage_connector_api.StorageConnectorApi()
        self._online_connector = None
        self._hostname = None
        self._connection_options = None

    def fetch_prepared_statements(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
        inference_helper_columns: bool,
    ) -> None:
        if isinstance(entity, feature_view.FeatureView):
            _logger.debug(
                f"Initialising prepared statements for feature view {entity.name} version {entity.version}."
            )
            for key in self.get_prepared_statement_labels(inference_helper_columns):
                _logger.debug(f"Fetching prepared statement for key {key}")
                self.prepared_statements[key] = (
                    self.feature_view_api.get_serving_prepared_statement(
                        entity.name,
                        entity.version,
                        batch=key.startswith("batch"),
                        inference_helper_columns=key.endswith("helper_column"),
                    )
                )
                _logger.debug(f"{self.prepared_statements[key]}")
        elif isinstance(entity, training_dataset.TrainingDataset):
            _logger.debug(
                f"Initialising prepared statements for training dataset {entity.name} version {entity.version}."
            )
            for key in self.get_prepared_statement_labels(
                with_inference_helper_column=False
            ):
                _logger.debug(f"Fetching prepared statement for key {key}")
                self.prepared_statements[key] = (
                    self.training_dataset_api.get_serving_prepared_statement(
                        entity, batch=key.startswith("batch")
                    )
                )
        else:
            raise ValueError(
                "Object type needs to be `feature_view.FeatureView` or `training_dataset.TrainingDataset`."
            )

        if len(self.skip_fg_ids) > 0:
            _logger.debug(
                f"Skip feature groups {self.skip_fg_ids} when initialising prepared statements."
            )
            self.prepared_statements[key] = {
                ps
                for ps in self.prepared_statements[key]
                if ps.feature_group_id not in self.skip_fg_ids
            }

    def init_prepared_statements(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
        inference_helper_columns: bool,
    ) -> None:
        _logger.debug(
            "Fetch and reset prepared statements and external as user may be re-initialising with different parameters"
        )
        self.fetch_prepared_statements(entity, inference_helper_columns)

        self.init_parametrize_and_serving_utils(
            self.prepared_statements[self.BATCH_VECTOR_KEY]
        )

        for key in self.get_prepared_statement_labels(inference_helper_columns):
            _logger.debug(f"Parametrize prepared statements for key {key}")
            self._parametrised_prepared_statements[key] = (
                self._parametrize_prepared_statements(
                    self.prepared_statements[key], batch=key.startswith("batch")
                )
            )

    def init_parametrize_and_serving_utils(
        self,
        prepared_statements: List[ServingPreparedStatement],
    ) -> None:
        _logger.debug(
            "Initializing parametrize and serving utils property using %s",
            json.dumps(prepared_statements, default=lambda x: x.__dict__, indent=2),
        )
        self.prefix_by_serving_index = {
            statement.prepared_statement_index: statement.prefix
            for statement in prepared_statements
        }
        self._feature_name_order_by_psp = {
            statement.prepared_statement_index: dict(
                [
                    (param.name, param.index)
                    for param in statement.prepared_statement_parameters
                ]
            )
            for statement in prepared_statements
        }

        _logger.debug("Build serving keys by PreparedStatementParameter.index")
        for sk in self._serving_keys:
            self.serving_key_by_serving_index[sk.join_index] = (
                self.serving_key_by_serving_index.get(sk.join_index, []) + [sk]
            )
        _logger.debug("Sort serving keys by PreparedStatementParameter.index")
        for join_index in self.serving_key_by_serving_index:
            # feature_name_order_by_psp do not include the join index when the joint feature only contains label only
            # But _serving_key_by_serving_index include the index when the join_index is 0 (left side)
            if join_index in self._feature_name_order_by_psp:
                self.serving_key_by_serving_index[join_index] = sorted(
                    self.serving_key_by_serving_index[join_index],
                    key=lambda _sk,
                    join_index=join_index: self.feature_name_order_by_psp[
                        join_index
                    ].get(_sk.feature_name, 0),
                )

    def _parametrize_prepared_statements(
        self,
        prepared_statements: List[ServingPreparedStatement],
        batch: bool,
    ) -> Dict[int, sql.text]:
        prepared_statements_dict = {}
        for prepared_statement in prepared_statements:
            if prepared_statement.feature_group_id in self._skip_fg_ids:
                continue
            query_online = str(prepared_statement.query_online).replace("\n", " ")

            if not batch:
                for param in prepared_statement.prepared_statement_parameters:
                    query_online = self._parametrize_query(param.name, query_online)
                query_online = sql.text(query_online)
            else:
                query_online = self._parametrize_query("batch_ids", query_online)
                query_online = sql.text(query_online)
                query_online = query_online.bindparams(
                    batch_ids=bindparam("batch_ids", expanding=True)
                )

            prepared_statements_dict[prepared_statement.prepared_statement_index] = (
                query_online
            )

        return prepared_statements_dict

    def init_async_mysql_connection(self, options=None):
        assert self._prepared_statements.get(self.SINGLE_VECTOR_KEY) is not None, (
            "Prepared statements are not initialized. "
            "Please call `init_prepared_statement` method first."
        )
        _logger.debug(
            "Fetching storage connector for sql connection to Online Feature Store."
        )
        self._online_connector = self._storage_connector_api.get_online_connector(
            self._feature_store_id
        )
        self._connection_options = options
        self._hostname = util.get_host_name() if self._external else None

        if util.is_runtime_notebook():
            _logger.debug("Running in Jupyter notebook, applying nest_asyncio")
            import nest_asyncio

            nest_asyncio.apply()
        else:
            _logger.debug("Running in python script. Not applying nest_asyncio")

    def get_single_feature_vector(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve single vector with parallel queries using aiomysql engine."""
        return self._single_vector_result(
            entry, self.parametrised_prepared_statements[self.SINGLE_VECTOR_KEY]
        )

    def get_batch_feature_vectors(
        self, entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Retrieve batch vector with parallel queries using aiomysql engine."""
        return self._batch_vector_results(
            entries, self.parametrised_prepared_statements[self.BATCH_VECTOR_KEY]
        )

    def get_inference_helper_vector(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve single vector with parallel queries using aiomysql engine."""
        return self._single_vector_result(
            entry, self.parametrised_prepared_statements[self.SINGLE_HELPER_KEY]
        )

    def get_batch_inference_helper_vectors(
        self, entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Retrieve batch vector with parallel queries using aiomysql engine."""
        return self._batch_vector_results(
            entries, self.parametrised_prepared_statements[self.BATCH_HELPER_KEY]
        )

    def _single_vector_result(
        self, entry: Dict[str, Any], prepared_statement_objects: Dict[int, sql.text]
    ) -> Dict[str, Any]:
        """Retrieve single vector with parallel queries using aiomysql engine."""

        if all([isinstance(val, list) for val in entry.values()]):
            raise ValueError(
                "Entry is expected to be single value per primary key. "
                "If you have already initialised prepared statements for single vector and now want to retrieve "
                "batch vector please reinitialise prepared statements with  "
                "`training_dataset.init_prepared_statement()` "
                "or `feature_view.init_serving()`"
            )
        # Initialize the set of values
        serving_vector = {}
        bind_entries = {}
        prepared_statement_execution = {}
        for prepared_statement_index in prepared_statement_objects:
            pk_entry = {}
            next_statement = False
            for sk in self.serving_key_by_serving_index[prepared_statement_index]:
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
            prepared_statement_execution[prepared_statement_index] = (
                prepared_statement_objects[prepared_statement_index]
            )

        # run all the prepared statements in parallel using aiomysql engine
        _logger.debug(
            f"Executing prepared statements for serving vector with entries: {bind_entries}"
        )
        loop = self._get_or_create_event_loop()
        results_dict = loop.run_until_complete(
            self._execute_prep_statements(prepared_statement_execution, bind_entries)
        )
        _logger.debug(f"Retrieved feature vectors: {results_dict}")
        _logger.debug("Constructing serving vector from results")
        for key in results_dict:
            for row in results_dict[key]:
                _logger.debug(f"Processing row: {row} for prepared statement {key}")
                result_dict = dict(row)
                serving_vector.update(result_dict)

        return serving_vector

    def _batch_vector_results(
        self,
        entries: List[Dict[str, Any]],
        prepared_statement_objects: Dict[int, sql.text],
    ):
        """Execute prepared statements in parallel using aiomysql engine."""
        _logger.debug(
            f"Starting batch vector retrieval for {len(entries)} entries via aiomysql engine."
        )
        # create dict object that will have of order of the vector as key and values as
        # vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
        # expect that backend will return correctly ordered vectors.
        batch_results = [{} for _ in range(len(entries))]
        entry_values = {}
        serving_keys_all_fg = []
        prepared_stmts_to_execute = {}
        # construct the list of entry values for binding to query
        _logger.debug(f"Parametrize prepared statements with entry values: {entries}")
        for prepared_statement_index in prepared_statement_objects:
            # prepared_statement_index include fg with label only
            # But _serving_key_by_serving_index include the index when the join_index is 0 (left side)
            if prepared_statement_index not in self._serving_key_by_serving_index:
                continue

            prepared_stmts_to_execute[prepared_statement_index] = (
                prepared_statement_objects[prepared_statement_index]
            )
            entry_values_tuples = list(
                map(
                    lambda e, prepared_statement_index=prepared_statement_index: tuple(
                        [
                            (
                                e.get(sk.required_serving_key)
                                # Check if there is any entry matched with feature name,
                                # if the required serving key is not provided.
                                or e.get(sk.feature_name)
                            )
                            for sk in self.serving_key_by_serving_index[
                                prepared_statement_index
                            ]
                        ]
                    ),
                    entries,
                )
            )
            _logger.debug(
                f"Prepared statement {prepared_statement_index} with entries: {entry_values_tuples}"
            )
            entry_values[prepared_statement_index] = {"batch_ids": entry_values_tuples}

        _logger.debug(
            f"Executing prepared statements for batch vector with entries: {entry_values}"
        )
        # run all the prepared statements in parallel using aiomysql engine
        loop = self._get_or_create_event_loop()
        parallel_results = loop.run_until_complete(
            self._execute_prep_statements(prepared_stmts_to_execute, entry_values)
        )

        _logger.debug(f"Retrieved feature vectors: {parallel_results}, stitching them.")
        # construct the results
        for prepared_statement_index in prepared_stmts_to_execute:
            statement_results = {}
            serving_keys = self.serving_key_by_serving_index[prepared_statement_index]
            serving_keys_all_fg += serving_keys
            prefix_features = [
                (self.prefix_by_serving_index[prepared_statement_index] or "")
                + sk.feature_name
                for sk in self.serving_key_by_serving_index[prepared_statement_index]
            ]
            _logger.debug(
                f"Use prefix from prepare statement because prefix from serving key is collision adjusted {prefix_features}."
            )
            _logger.debug("iterate over results by index of the prepared statement")
            for row in parallel_results[prepared_statement_index]:
                _logger.debug(f"Processing row: {row}")
                row_dict = dict(row)
                # can primary key be complex feature? No, not supported.
                result_dict = row_dict
                _logger.debug(
                    f"Add result to statement results: {self._get_result_key(prefix_features, row_dict)} : {result_dict}"
                )
                statement_results[self._get_result_key(prefix_features, row_dict)] = (
                    result_dict
                )

            _logger.debug(f"Add partial results to batch results: {statement_results}")
            for i, entry in enumerate(entries):
                _logger.debug(
                    "Processing entry %s : %s",
                    entry,
                    statement_results.get(
                        self._get_result_key_serving_key(serving_keys, entry), {}
                    ),
                )
                batch_results[i].update(
                    statement_results.get(
                        self._get_result_key_serving_key(serving_keys, entry), {}
                    )
                )
        return batch_results, serving_keys_all_fg

    def _get_or_create_event_loop(self):
        try:
            _logger.debug("Acquiring or starting event loop for async engine.")
            loop = asyncio.get_event_loop()
            asyncio.set_event_loop(loop)
        except RuntimeError as ex:
            if "There is no current event loop in thread" in str(ex):
                _logger.debug(
                    "No existing running event loop. Creating new event loop."
                )
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
        return loop

    def refresh_mysql_connection(self):
        _logger.debug("Refreshing MySQL connection.")
        try:
            _logger.debug("Checking if the connection is still alive.")
            with self._prepared_statement_engine.connect():
                # This will raise an exception if the connection is closed
                pass
        except exc.OperationalError:
            _logger.debug("Connection is closed, re-establishing connection.")
            self._set_mysql_connection()

    def _make_preview_statement(self, statement, n):
        return text(statement.text[: statement.text.find(" WHERE ")] + f" LIMIT {n}")

    def _set_mysql_connection(self, options=None):
        _logger.debug(
            "Retrieve MySQL connection details from the online storage connector."
        )
        online_conn = self._storage_connector_api.get_online_connector(
            self._feature_store_id
        )
        _logger.debug(
            f"Creating MySQL {'external' if self.external is True else ''}engine with options: {options}."
        )
        self._prepared_statement_engine = util.create_mysql_engine(
            online_conn, self._external, options=options
        )

    @staticmethod
    def _parametrize_query(name: str, query_online: str) -> str:
        # Now we have ordered pk_names, iterate over it and replace `?` with `:feature_name` one by one.
        # Regex `"^(.*?)\?"` will identify 1st occurrence of `?` in the sql string and replace it with provided
        # feature name, e.g. `:feature_name`:. As we iteratively update `query_online` we are always aiming to
        # replace 1st occurrence of `?`. This approach can only work if primary key names are sorted properly.
        # Regex `"^(.*?)\?"`:
        # `^` - asserts position at start of a line
        # `.*?` - matches any character (except for line terminators). `*?` Quantifier —
        # Matches between zero and unlimited times, expanding until needed, i.e 1st occurrence of `\?`
        # character.
        _logger.debug(f"Parametrizing name {name} in query {query_online}")
        return re.sub(
            r"^(.*?)\?",
            r"\1:" + name,
            query_online,
        )

    @staticmethod
    def _get_result_key(
        primary_keys: List[str], result_dict: Dict[str, str]
    ) -> Tuple[str]:
        _logger.debug(f"Get result key {primary_keys} from result dict {result_dict}")
        result_key = []
        for pk in primary_keys:
            result_key.append(result_dict.get(pk))
        return tuple(result_key)

    @staticmethod
    def _get_result_key_serving_key(
        serving_keys: List["ServingKey"], result_dict: Dict[str, Dict[str, Any]]
    ) -> Tuple[str]:
        _logger.debug(
            f"Get result key serving key {serving_keys} from result dict {result_dict}"
        )
        result_key = []
        for sk in serving_keys:
            _logger.debug(
                f"Get result key for serving key {sk.required_serving_key} or {sk.feature_name}"
            )
            result_key.append(
                result_dict.get(sk.required_serving_key)
                or result_dict.get(sk.feature_name)
            )
        _logger.debug(f"Result key: {result_key}")
        return tuple(result_key)

    @staticmethod
    def get_prepared_statement_labels(
        with_inference_helper_column: bool = False,
    ) -> List[str]:
        if with_inference_helper_column:
            return [
                OnlineStoreSqlClient.SINGLE_VECTOR_KEY,
                OnlineStoreSqlClient.BATCH_VECTOR_KEY,
                OnlineStoreSqlClient.SINGLE_HELPER_KEY,
                OnlineStoreSqlClient.BATCH_HELPER_KEY,
            ]
        return [
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY,
            OnlineStoreSqlClient.BATCH_VECTOR_KEY,
        ]

    async def _get_connection_pool(self, default_min_size: int) -> None:
        self._connection_pool = await util.create_async_engine(
            self._online_connector,
            self._external,
            default_min_size,
            options=self._connection_options,
            hostname=self._hostname,
        )

    async def _query_async_sql(self, stmt, bind_params):
        """Query prepared statement together with bind params using aiomysql connection pool"""
        if self._connection_pool is None:
            await self._get_connection_pool(
                len(self._prepared_statements[self.SINGLE_VECTOR_KEY])
            )
        async with self._connection_pool.acquire() as conn:
            # Execute the prepared statement
            _logger.debug(
                f"Executing prepared statement: {stmt} with bind params: {bind_params}"
            )
            cursor = await conn.execute(stmt, bind_params)
            # Fetch the result
            _logger.debug("Waiting for resultset.")
            resultset = await cursor.fetchall()
            _logger.debug(f"Retrieved resultset: {resultset}. Closing cursor.")
            await cursor.close()

        return resultset

    async def _execute_prep_statements(
        self,
        prepared_statements: Dict[int, str],
        entries: Union[List[Dict[str, Any]], Dict[str, Any]],
    ):
        """Iterate over prepared statements to create async tasks
        and gather all tasks results for a given list of entries."""

        # validate if prepared_statements and entries have the same keys
        if prepared_statements.keys() != entries.keys():
            # iterate over prepared_statements and entries to find the missing key
            # remove missing keys from prepared_statements
            for key in list(prepared_statements.keys()):
                if key not in entries:
                    prepared_statements.pop(key)

        try:
            tasks = [
                asyncio.create_task(
                    self._query_async_sql(prepared_statements[key], entries[key]),
                    name="query_prep_statement_key" + str(key),
                )
                for key in prepared_statements
            ]
            # Run the queries in parallel using asyncio.gather
            results = await asyncio.gather(*tasks)
        except asyncio.CancelledError as e:
            _logger.error(f"Failed executing prepared statements: {e}")
            raise e

        # Create a dict of results with the prepared statement index as key
        results_dict = {}
        for i, key in enumerate(prepared_statements):
            results_dict[key] = results[i]

        return results_dict

    @property
    def feature_store_id(self) -> int:
        return self._feature_store_id

    @property
    def prepared_statement_engine(self) -> Optional[Any]:
        """JDBC connection engine to retrieve connections to online features store from."""
        return self._prepared_statement_engine

    @prepared_statement_engine.setter
    def prepared_statement_engine(self, prepared_statement_engine: Any) -> None:
        self._prepared_statement_engine = prepared_statement_engine

    @property
    def prepared_statements(
        self,
    ) -> Dict[str, List[ServingPreparedStatement]]:
        """Contains up to 4 prepared statements for single and batch vector retrieval, and single or batch inference helpers.

        The keys are the labels for the prepared statements, and the values are dictionaries of prepared statements
        with the prepared statement index as the key.
        """
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(
        self,
        prepared_statements: Dict[str, List[ServingPreparedStatement]],
    ) -> None:
        self._prepared_statements = prepared_statements

    @property
    def parametrised_prepared_statements(
        self,
    ) -> Dict[str, Dict[int, sql.text]]:
        """The dict object of prepared_statements as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset. Used for batch retrieval.
        """
        return self._parametrised_prepared_statements

    @parametrised_prepared_statements.setter
    def parametrised_prepared_statements(
        self,
        parametrised_prepared_statements: Dict[str, Dict[int, sql.text]],
    ) -> None:
        self._parametrised_prepared_statements = parametrised_prepared_statements

    @property
    def prefix_by_serving_index(self) -> Dict[int, str]:
        """The dict object of prefixes as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._prefix_by_serving_index

    @prefix_by_serving_index.setter
    def prefix_by_serving_index(self, prefix_by_serving_index: Dict[int, str]) -> None:
        _logger.debug(f"Setting prefix by serving index {prefix_by_serving_index}.")
        self._prefix_by_serving_index = prefix_by_serving_index

    @property
    def serving_key_by_serving_index(
        self,
    ) -> Dict[int, List[ServingKey]]:
        """The dict object of serving keys as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._serving_key_by_serving_index

    @property
    def feature_name_order_by_psp(self) -> Dict[int, Dict[str, int]]:
        """The dict object of feature names as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._feature_name_order_by_psp

    @property
    def skip_fg_ids(self) -> Set[int]:
        """The list of feature group ids to skip when retrieving feature vectors.

        The retrieval of Feature values stored in Feature Group with embedding is handled via a separate client
        as there are not stored in RonDB.
        """
        return self._skip_fg_ids

    @property
    def serving_keys(self) -> Set[ServingKey]:
        if len(self._serving_keys) > 0:
            return self._serving_keys

        if len(self.prepared_statements) == 0:
            raise ValueError(
                "Prepared statements are not initialized. Please call `init_prepared_statement` method first."
            )
        else:
            _logger.debug(
                "Build serving keys from prepared statements ignoring prefix to ensure compatibility with older version."
            )
            self._serving_keys = util.build_serving_keys_from_prepared_statements(
                self.prepared_statements[
                    self.BATCH_VECTOR_KEY
                ],  # use batch to avoid issue with label_fg
                ignore_prefix=True,  # if serving_keys are not set it is because the feature view is anterior to 3.3, this ensures compatibility
            )
        return self._serving_keys

    @property
    def training_dataset_api(self) -> training_dataset_api.TrainingDatasetApi:
        return self._training_dataset_api

    @property
    def feature_view_api(self) -> feature_view_api.FeatureViewApi:
        return self._feature_view_api

    @property
    def storage_connector_api(self) -> storage_connector_api.StorageConnectorApi:
        return self._storage_connector_api

    @property
    def hostname(self) -> str:
        return self._hostname

    @property
    def connection_options(self) -> Dict[str, Any]:
        return self._connection_options

    @property
    def online_connector(self) -> storage_connector.StorageConnector:
        return self._online_connector

    @property
    def connection_pool(self) -> aiomysql.utils._ConnectionContextManager:
        return self._connection_pool
