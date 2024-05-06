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
from __future__ import annotations

import io
import logging
from typing import Any, Dict, List, Optional, Set, Union

import avro.io
import avro.schema
import numpy as np
import pandas as pd
import polars as pl
from hsfs import (
    client,
    feature_view,
    training_dataset,
    transformation_function_attached,
)
from hsfs import serving_key as sk_mod
from hsfs import training_dataset_feature as tdf_mod
from hsfs.core import (
    online_store_sql_client,
    transformation_function_engine,
)


_logger = logging.getLogger(__name__)


class VectorServer:
    def __init__(
        self,
        feature_store_id: int,
        features: Optional[List[tdf_mod.TrainingDatasetFeature]] = None,
        training_dataset_version: Optional[int] = None,
        serving_keys: Optional[List[sk_mod.ServingKey]] = None,
        skip_fg_ids: Optional[Set[int]] = None,
    ):
        self._training_dataset_version = training_dataset_version
        self._feature_store_id = feature_store_id

        if features is None:
            features = []

        self._features = features
        self._feature_vector_col_name = [
            feat.name
            for feat in features
            if not (
                feat.label
                or feat.inference_helper_column
                or feat.training_helper_column
            )
        ]
        self._inference_helper_col_name = [
            feat.name for feat in features if feat.inference_helper_column
        ]
        self._transformed_feature_vector_col_name: List[str] = None

        self._skip_fg_ids = skip_fg_ids or set()
        self._serving_keys = serving_keys or []
        self._required_serving_keys = []

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                feature_store_id
            )
        )
        self._transformation_functions = None
        self._online_store_sql_client: Optional[
            online_store_sql_client.OnlineStoreSqlClient
        ] = None

    def init_serving(
        self,
        entity: Union[feature_view.FeatureView],
        external: Optional[bool] = None,
        inference_helper_columns: bool = False,
        options: Optional[Dict[str, Any]] = None,
    ):
        if external is None:
            external = isinstance(client.get_instance(), client.external.Client)
        # `init_prepared_statement` should be the last because other initialisations
        # has to be done successfully before it is able to fetch feature vectors.
        self.init_transformation(entity)
        self._complex_features = self.get_complex_feature_schemas()

        self.setup_online_store_sql_client(
            entity=entity,
            external=external,
            inference_helper_columns=inference_helper_columns,
            options=options,
        )

    def init_batch_scoring(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
    ):
        self.init_transformation(entity)

    def init_transformation(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
    ):
        # attach transformation functions
        self._transformation_functions = (
            self.transformation_function_engine.get_ready_to_use_transformation_fns(
                entity,
                self._training_dataset_version,
            )
        )

    def setup_online_store_sql_client(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
        external: bool,
        inference_helper_columns: bool,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        _logger.info("Online SQL client")
        self._online_store_sql_client = online_store_sql_client.OnlineStoreSqlClient(
            feature_store_id=self._feature_store_id,
            skip_fg_ids=self._skip_fg_ids,
            serving_keys=self.serving_keys,
            external=external,
        )
        self.online_store_sql_client.init_prepared_statements(
            entity,
            inference_helper_columns,
        )
        self.online_store_sql_client.init_async_mysql_connection(options=options)

    def get_feature_vector(
        self,
        entry: Dict[str, Any],
        return_type: Optional[str] = None,
        passed_features: Optional[Dict[str, Any]] = None,
        vector_db_features: Optional[Dict[str, Any]] = None,
        td_embedding_feature_names: Optional[set[str]] = None,
        allow_missing: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, np.ndarray, List[Any], Dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        if passed_features is None:
            passed_features = {}
        if td_embedding_feature_names is None:
            td_embedding_feature_names = set()
        # get result row
        _logger.debug("get_feature_vector Online SQL client")
        serving_vector = self.online_store_sql_client.get_single_feature_vector(entry)
        if vector_db_features:
            serving_vector.update(vector_db_features)
        # Deserialize complex features
        _logger.debug("Deserializing complex features")
        serving_vector = self.deserialize_complex_features(
            serving_vector, td_embedding_feature_names
        )

        # Add the passed features
        _logger.debug("Updating with passed features")
        serving_vector.update(passed_features)

        # apply transformation functions
        _logger.debug("Applying transformation functions")
        result_dict = self._apply_transformation(serving_vector)

        _logger.debug(
            "Converting to row vectors to list, optionally filling missing values"
        )
        vector = self._generate_vector(result_dict, allow_missing)

        return self.handle_feature_vector_return_type(
            vector, batch=False, inference_helper=False, return_type=return_type
        )

    def get_feature_vectors(
        self,
        entries: List[Dict[str, Any]],
        return_type: Optional[str] = None,
        passed_features: Optional[List[Dict[str, Any]]] = None,
        vector_db_features: Optional[List[Dict[str, Any]]] = None,
        td_embedding_feature_names: Optional[set[str]] = None,
        allow_missing: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, np.ndarray, List[Any], List[Dict[str, Any]]]:
        if passed_features is None:
            passed_features = []
        if td_embedding_feature_names is None:
            td_embedding_feature_names = set()
        """Assembles serving vector from online feature store."""
        _logger.debug("get_feature_vectors through SQL client")
        batch_results, _ = self.online_store_sql_client.get_batch_feature_vectors(
            entries
        )
        if vector_db_features:
            for i in range(len(batch_results)):
                batch_results[i].update(vector_db_features[i])
        # Deserialize complex features
        _logger.debug("Deserializing complex features")
        batch_results = list(
            map(
                lambda row_dict: self.deserialize_complex_features(
                    row_dict, td_embedding_feature_names
                ),
                batch_results,
            )
        )

        # apply passed features to each batch result
        _logger.debug("Updating with passed features")
        for vector_index, pf in enumerate(passed_features):
            batch_results[vector_index].update(pf)

        # apply transformation functions
        _logger.debug("Applying transformation functions")
        batch_transformed = list(
            map(
                lambda results_dict: self._apply_transformation(results_dict),
                batch_results,
            )
        )

        # get vectors
        _logger.debug(
            "Converting to row vectors to list, optionally filling missing values"
        )
        vectors = []
        for result in batch_transformed:
            # for backward compatibility, before 3.4, if result is empty,
            # instead of throwing error, it skips the result
            if len(result) != 0 or allow_missing:
                vectors.append(self._generate_vector(result, fill_na=allow_missing))

        return self.handle_feature_vector_return_type(
            vectors, batch=True, inference_helper=False, return_type=return_type
        )

    def handle_feature_vector_return_type(
        self,
        feature_vectorz: Union[
            List[Any], List[List[Any]], Dict[str, Any], List[Dict[str, Any]]
        ],
        batch: bool,
        inference_helper: bool,
        return_type: str,
    ) -> Union[
        pd.DataFrame,
        pl.DataFrame,
        np.ndarray,
        List[Any],
        Dict[str, Any],
        List[Dict[str, Any]],
    ]:
        # Only get-feature-vector and get-feature-vectors can return list or numpy
        if return_type.lower() == "list" and not inference_helper:
            _logger.debug("Returning feature vector as value list")
            return feature_vectorz
        elif return_type.lower() == "numpy" and not inference_helper:
            _logger.debug("Returning feature vector as numpy array")
            return np.array(feature_vectorz)
        # Only inference helper can return dict
        elif return_type.lower() == "dict" and inference_helper:
            _logger.debug("Returning feature vector as dictionary")
            return feature_vectorz
        # Both can return pandas and polars
        elif return_type.lower() == "pandas":
            _logger.debug("Returning feature vector as pandas dataframe")
            if batch and inference_helper:
                return pd.DataFrame(feature_vectorz)
            elif inference_helper:
                return pd.DataFrame([feature_vectorz])
            elif batch:
                return pd.DataFrame(
                    feature_vectorz, columns=self._feature_vector_col_name
                )
            else:
                pandas_df = pd.DataFrame(feature_vectorz).transpose()
                pandas_df.columns = self._feature_vector_col_name
                return pandas_df
        elif return_type.lower() == "polars":
            _logger.debug("Returning feature vector as polars dataframe")
            return pl.DataFrame(
                feature_vectorz if batch else [feature_vectorz],
                schema=self._feature_vector_col_name if not inference_helper else None,
                orient="row",
            )
        else:
            raise ValueError(
                f"""Unknown return type. Supported return types are {"'list', 'numpy'" if not inference_helper else "'dict'"}, 'polars' and 'pandas''"""
            )

    def get_inference_helper(
        self, entry: Dict[str, Any], return_type: str
    ) -> Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        _logger.debug("Retrieve inference helper values for single entry.")
        _logger.debug(f"entry: {entry} as return type: {return_type}")
        return self.handle_feature_vector_return_type(
            self.online_store_sql_client.get_inference_helper_vector(entry),
            batch=False,
            inference_helper=True,
            return_type=return_type,
        )

    def get_inference_helpers(
        self,
        feature_view_object: feature_view.FeatureView,
        entries: List[Dict[str, Any]],
        return_type: str,
    ) -> Union[pd.DataFrame, pl.DataFrame, List[Dict[str, Any]]]:
        """Assembles serving vector from online feature store."""
        batch_results, serving_keys = (
            self.online_store_sql_client.get_batch_inference_helper_vectors(entries)
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

        return self.handle_feature_vector_return_type(
            batch_results, batch=True, inference_helper=True, return_type=return_type
        )

    def get_complex_feature_schemas(self) -> Dict[str, avro.io.DatumReader]:
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

    def deserialize_complex_features(
        self, row_dict: Dict[str, Any], td_embedding_feature_names: set[str]
    ) -> Dict[str, Any]:
        for feature_name, schema in self._complex_features.items():
            if (
                feature_name in row_dict
                and feature_name not in td_embedding_feature_names
            ):
                bytes_reader = io.BytesIO(row_dict[feature_name])
                decoder = avro.io.BinaryDecoder(bytes_reader)
                row_dict[feature_name] = schema.read(decoder)
        return row_dict

    def _generate_vector(self, result_dict: Dict[str, Any], fill_na: bool = False):
        feature_values = []
        for feature_name in self._feature_vector_col_name:
            if feature_name not in result_dict:
                if fill_na:
                    feature_values.append(None)
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
                feature_values.append(result_dict[feature_name])
        return feature_values

    def _apply_transformation(self, row_dict: dict):
        # TODO : Clean this up and optimize
        for transformation_function in self._transformation_functions:
            features = [
                pd.Series(row_dict[feature])
                for feature in transformation_function.hopsworks_udf.transformation_features
            ]
            transformed_result = transformation_function.hopsworks_udf.get_udf()(
                *features
            )
            if isinstance(transformed_result, pd.Series):
                row_dict[transformed_result.name] = transformed_result.values[0]
            else:
                for col in transformed_result:
                    row_dict[transformed_result.name] = transformed_result[col].values[
                        0
                    ]
        return row_dict

    @property
    def online_store_sql_client(
        self,
    ) -> Optional[online_store_sql_client.OnlineStoreSqlClient]:
        return self._online_store_sql_client
    

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

    def _get_transformation_fns(self, fv: feature_view.FeatureView):
        # get attached transformation functions
        transformation_functions = (
            self._feature_view_engine.get_attached_transformation_fn(
                fv.name, fv.version
            )
        )
        is_stat_required = any(
            [tf.hopsworks_udf.statistics_required for tf in fv.transformation_functions]
        )
        is_feat_view = isinstance(fv, feature_view.FeatureView)
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
                fv,
                before_transformation=True,
                training_dataset_version=self._training_dataset_version,
            )
        # TODO : clean this up
        if is_stat_required and td_tffn_stats is None:
            raise ValueError(
                "No statistics available for initializing transformation functions."
            )
        if is_stat_required:
            for transformation_function in transformation_functions:
                transformation_function.hopsworks_udf.transformation_statistics = (
                    td_tffn_stats.feature_descriptive_statistics
                )

        return transformation_functions

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
    def serving_keys(self) -> List[sk_mod.ServingKey]:
        return self._serving_keys

    @serving_keys.setter
    def serving_keys(self, serving_vector_keys: List[sk_mod.ServingKey]):
        self._serving_keys = serving_vector_keys

    @property
    def required_serving_keys(self) -> List[str]:
        if len(self._required_serving_keys) == 0:
            self._required_serving_keys = [
                sk.required_serving_key for sk in self.serving_keys
            ]
        return self._required_serving_keys

    @property
    def training_dataset_version(self) -> Optional[int]:
        return self._training_dataset_version

    @training_dataset_version.setter
    def training_dataset_version(self, training_dataset_version: Optional[int]):
        self._training_dataset_version = training_dataset_version

    @property
    def transformation_functions(
        self,
    ) -> Optional[
        Dict[str, transformation_function_attached.TransformationFunctionAttached]
    ]:
        return self._transformation_functions

    @property
    def transformation_function_engine(
        self,
    ) -> transformation_function_engine.TransformationFunctionEngine:
        return self._transformation_function_engine

    def transformed_feature_vector_col_name(self):
        if self._transformed_feature_vector_col_name is None:
            self._transformed_feature_vector_col_name = self._feature_vector_col_name
            for transformation_function in self._transformation_functions:
                self._transformed_feature_vector_col_name += (
                    transformation_function.hopsworks_udf.transformation_features
                )
        return self._transformed_feature_vector_col_name
