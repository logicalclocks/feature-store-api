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

import itertools
import logging
from base64 import b64decode
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Union

import avro.io
import avro.schema
import numpy as np
import pandas as pd
import polars as pl
from hsfs import (
    client,
    feature_view,
    training_dataset,
    transformation_function,
)
from hsfs import (
    serving_key as sk_mod,
)
from hsfs import training_dataset_feature as tdf_mod
from hsfs.client import exceptions, online_store_rest_client
from hsfs.core import (
    online_store_rest_client_engine,
    online_store_sql_engine,
)
from hsfs.core import (
    transformation_function_engine as tf_engine_mod,
)


HAS_FASTAVRO = False
try:
    from fastavro import schemaless_reader

    HAS_FASTAVRO = True
except ImportError:
    from avro.io import BinaryDecoder

_logger = logging.getLogger(__name__)


class VectorServer:
    DEFAULT_ONLINE_STORE_REST_CLIENT = "rest"
    DEFAULT_ONLINE_STORE_SQL_CLIENT = "sql"
    DEFAULT_ONLINE_STORE_CLIENT_KEY = "default_online_store_client"
    ONLINE_REST_CLIENT_CONFIG_OPTIONS_KEY = "config_online_store_rest_client"
    RESET_ONLINE_REST_CLIENT_OPTIONS_KEY = "reset_online_store_rest_client"
    SQL_TIMESTAMP_STRING_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        feature_store_id: int,
        features: Optional[List[tdf_mod.TrainingDatasetFeature]] = None,
        training_dataset_version: Optional[int] = None,
        serving_keys: Optional[List[sk_mod.ServingKey]] = None,
        skip_fg_ids: Optional[Set[int]] = None,
        feature_store_name: Optional[str] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ):
        self._training_dataset_version = training_dataset_version
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

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
            tf_engine_mod.TransformationFunctionEngine(feature_store_id)
        )
        self._transformation_functions: List[
            transformation_function.TransformationFunction
        ] = []
        self._online_store_sql_client = None

        self._online_store_rest_client_engine = None
        self._init_online_store_rest_client: Optional[bool] = None
        self._init_online_store_sql_client: Optional[bool] = None
        self._default_online_store_client: Optional[Literal["rest", "sql"]] = None

        self.set_return_feature_value_handlers(features=self._features)

    def init_serving(
        self,
        entity: Union[feature_view.FeatureView],
        external: Optional[bool] = None,
        inference_helper_columns: bool = False,
        options: Optional[Dict[str, Any]] = None,
        init_online_store_sql_client: Optional[bool] = None,
        init_online_store_rest_client: bool = False,
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
        self.set_return_feature_value_handlers(features=entity.features)

        if self._init_online_store_rest_client:
            self.setup_online_store_rest_client_and_engine(
                entity=entity, options=options
            )

        if self._init_online_store_sql_client:
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
        entity: Union[feature_view.FeatureView],
    ):
        # attach transformation functions
        self._transformation_functions = tf_engine_mod.TransformationFunctionEngine.get_ready_to_use_transformation_fns(
            entity,
            self._training_dataset_version,
        )

    def setup_online_store_sql_client(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
        external: bool,
        inference_helper_columns: bool,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        _logger.info("Initialising Vector Server Online Store SQL client")
        self._online_store_sql_client = online_store_sql_engine.OnlineStoreSqlClient(
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

    def setup_online_store_rest_client_and_engine(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
        options: Optional[Dict[str, Any]] = None,
    ):
        # naming is off here, but it avoids confusion with the argument init_online_store_rest_client
        _logger.info("Initialising Vector Server Online Store REST client")
        self._online_store_rest_client_engine = (
            online_store_rest_client_engine.OnlineStoreRestClientEngine(
                feature_store_name=self._feature_store_name,
                feature_view_name=entity.name,
                feature_view_version=entity.version,
                features=entity.features,
            )
        )
        # This logic needs to move to the above engine init
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

        online_store_rest_client.init_or_reset_online_store_rest_client(
            optional_config=online_store_rest_client_config,
            reset_client=reset_online_rest_client,
        )

    def get_feature_vector(
        self,
        entry: Dict[str, Any],
        return_type: Union[Literal["list", "numpy", "pandas", "polars"]],
        passed_features: Optional[Dict[str, Any]] = None,
        vector_db_features: Optional[Dict[str, Any]] = None,
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, np.ndarray, List[Any], Dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        online_client_choice = self.which_client_and_ensure_initialised(
            force_rest_client=force_rest_client, force_sql_client=force_sql_client
        )
        rondb_entry = self.validate_entry(
            entry=entry,
            allow_missing=allow_missing,
            passed_features=passed_features,
            vector_db_features=vector_db_features,
        )
        if len(rondb_entry) == 0:
            _logger.debug("Empty entry for rondb, skipping fetching.")
            serving_vector = {}  # updated below with vector_db_features and passed_features
        elif online_client_choice == self.DEFAULT_ONLINE_STORE_REST_CLIENT:
            _logger.debug("get_feature_vector Online REST client")
            serving_vector = self.online_store_rest_client_engine.get_single_feature_vector(
                rondb_entry,
                drop_missing=not allow_missing,
                return_type=self.online_store_rest_client_engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
        else:
            _logger.debug("get_feature_vector Online SQL client")
            serving_vector = self.online_store_sql_client.get_single_feature_vector(
                rondb_entry
            )

        vector = self.assemble_feature_vector(
            result_dict=serving_vector,
            passed_values=passed_features or {},
            vector_db_result=vector_db_features or {},
            allow_missing=allow_missing,
        )

        return self.handle_feature_vector_return_type(
            vector, batch=False, inference_helper=False, return_type=return_type
        )

    def get_feature_vectors(
        self,
        entries: List[Dict[str, Any]],
        return_type: Optional[
            Union[Literal["list", "numpy", "pandas", "polars"]]
        ] = None,
        passed_features: Optional[List[Dict[str, Any]]] = None,
        vector_db_features: Optional[List[Dict[str, Any]]] = None,
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, np.ndarray, List[Any], List[Dict[str, Any]]]:
        """Assembles serving vector from online feature store."""
        if passed_features is None:
            passed_features = []
        # Assertions on passed_features and vector_db_features
        assert (
            passed_features is None
            or len(passed_features) == 0
            or len(passed_features) == len(entries)
        ), "Passed features should be None, empty or have the same length as the entries"
        assert (
            vector_db_features is None
            or len(vector_db_features) == 0
            or len(vector_db_features) == len(entries)
        ), "Vector DB features should be None, empty or have the same length as the entries"

        online_client_choice = self.which_client_and_ensure_initialised(
            force_rest_client=force_rest_client, force_sql_client=force_sql_client
        )
        rondb_entries = []
        skipped_empty_entries = []
        for (idx, entry), passed, vector_features in itertools.zip_longest(
            enumerate(entries),
            passed_features,
            vector_db_features,
        ):
            rondb_entry = self.validate_entry(
                entry=entry,
                allow_missing=allow_missing,
                passed_features=passed,
                vector_db_features=vector_features,
            )
            if len(rondb_entry) != 0:
                rondb_entries.append(rondb_entry)
            else:
                skipped_empty_entries.append(idx)

        if (
            online_client_choice == self.DEFAULT_ONLINE_STORE_REST_CLIENT
            and len(rondb_entries) > 0
        ):
            _logger.debug("get_batch_feature_vector Online REST client")
            batch_results = self.online_store_rest_client_engine.get_batch_feature_vectors(
                entries=rondb_entries,
                drop_missing=not allow_missing,
                return_type=self.online_store_rest_client_engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
        elif len(rondb_entries) > 0:
            # get result row
            _logger.debug("get_batch_feature_vectors through SQL client")
            batch_results, _ = self.online_store_sql_client.get_batch_feature_vectors(
                rondb_entries
            )
        else:
            _logger.debug("Empty entries for rondb, skipping fetching.")
            batch_results = []

        _logger.debug("Assembling feature vectors from batch results")
        next_skipped = (
            skipped_empty_entries.pop(0) if len(skipped_empty_entries) > 0 else None
        )
        vectors = []
        for (
            idx,
            passed_values,
            vector_db_result,
        ) in itertools.zip_longest(
            range(len(entries)),
            passed_features or [],
            vector_db_features or [],
            fillvalue=None,
        ):
            if next_skipped == idx:
                _logger.debug("Entry %d was skipped, setting to empty dict.", idx)
                next_skipped = (
                    skipped_empty_entries.pop(0)
                    if len(skipped_empty_entries) > 0
                    else None
                )
                result_dict = {}
            else:
                result_dict = batch_results.pop(0)

            vector = self.assemble_feature_vector(
                result_dict=result_dict,
                passed_values=passed_values,
                vector_db_result=vector_db_result,
                allow_missing=allow_missing,
            )

            if vector is not None:
                vectors.append(vector)

        return self.handle_feature_vector_return_type(
            vectors, batch=True, inference_helper=False, return_type=return_type
        )

    def assemble_feature_vector(
        self,
        result_dict: Dict[str, Any],
        passed_values: Optional[Dict[str, Any]],
        vector_db_result: Optional[Dict[str, Any]],
        allow_missing: bool,
    ) -> Optional[List[Any]]:
        """Assembles serving vector from online feature store."""
        # Errors in batch requests are returned as None values
        _logger.debug("Assembling serving vector: %s", result_dict)
        if result_dict is None:
            _logger.debug("Found null result, setting to empty dict.")
            result_dict = {}
        if vector_db_result is not None and len(vector_db_result) > 0:
            _logger.debug("Updating with vector_db features: %s", vector_db_result)
            result_dict.update(vector_db_result)
        if passed_values is not None and len(passed_values) > 0:
            _logger.debug("Updating with passed features: %s", passed_values)
            result_dict.update(passed_values)

        missing_features = set(self.feature_vector_col_name).difference(
            result_dict.keys()
        )
        # for backward compatibility, before 3.4, if result is empty,
        # instead of throwing error, it skips the result
        # Maybe we drop this behaviour for 4.0
        if len(result_dict) == 0 and not allow_missing:
            return None

        if not allow_missing and len(missing_features) > 0:
            raise exceptions.FeatureStoreException(
                f"Feature(s) {str(missing_features)} is missing from vector."
                "Possible reasons: "
                "1. There is no match in the given entry."
                " Please check if the entry exists in the online feature store"
                " or provide the feature as passed_feature. "
                f"2. Required entries [{', '.join(self.required_serving_keys)}] or "
                f"[{', '.join(set(sk.feature_name for sk in self._serving_keys))}] are not provided."
            )

        if len(self.return_feature_value_handlers) > 0:
            self.apply_return_value_handlers(result_dict)
        if len(self.transformation_functions) > 0:
            self.apply_transformation(result_dict)

        _logger.debug("Assembled and transformed dict feature vector: %s", result_dict)

        return [
            result_dict.get(fname, None)
            for fname in self.transformed_feature_vector_col_name
        ]

    def handle_feature_vector_return_type(
        self,
        feature_vectorz: Union[
            List[Any], List[List[Any]], Dict[str, Any], List[Dict[str, Any]]
        ],
        batch: bool,
        inference_helper: bool,
        return_type: Union[Literal["list", "dict", "numpy", "pandas", "polars"]],
    ) -> Union[
        pd.DataFrame,
        pl.DataFrame,
        np.ndarray,
        List[Any],
        List[List[Any]],
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
        _logger.debug("Retrieve inference helper values for batch entries.")
        _logger.debug(f"entries: {entries} as return type: {return_type}")
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
            self._init_online_store_sql_client = True

    def apply_transformation(self, row_dict: dict):
        _logger.debug("Applying transformation functions.")
        for tf in self.transformation_functions:
            features = [
                pd.Series(row_dict[feature])
                for feature in tf.hopsworks_udf.transformation_features
            ]
            transformed_result = tf.hopsworks_udf.get_udf(force_python_udf=True)(
                *features
            )  # Get only python compatible UDF irrespective of engine
            if isinstance(transformed_result, pd.Series):
                row_dict[transformed_result.name] = transformed_result.values[0]
            else:
                for col in transformed_result:
                    row_dict[col] = transformed_result[col].values[0]
        return row_dict

    def apply_return_value_handlers(self, row_dict: Dict[str, Any]):
        matching_keys = set(self.return_feature_value_handlers.keys()).intersection(
            row_dict.keys()
        )
        _logger.debug("Applying return value handlers to : %s", matching_keys)
        for fname in matching_keys:
            row_dict[fname] = self.return_feature_value_handlers[fname](row_dict[fname])
        return row_dict

    def build_complex_feature_decoders(self) -> Dict[str, Callable]:
        """Build a dictionary of functions to deserialize or convert feature values.

        Handles:
            - deserialization of complex features from the online feature store
            - conversion of string or int timestamps to datetime objects
        """
        complex_feature_schemas = {
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

        if len(complex_feature_schemas) == 0:
            return {}
        else:
            _logger.debug(
                f"Building complex feature decoders corresponding to {complex_feature_schemas}."
            )
        if HAS_FASTAVRO:
            _logger.debug("Using fastavro for deserialization.")
            return {
                f_name: (
                    lambda feature_value, avro_schema=schema: (
                        schemaless_reader(
                            BytesIO(
                                feature_value
                                if isinstance(feature_value, bytes)
                                else b64decode(feature_value)
                            ),
                            avro_schema.writers_schema.to_json(),
                        )
                        # embedded features are deserialized already but not complex features stored in Opensearch
                        if (
                            isinstance(feature_value, bytes)
                            or isinstance(feature_value, str)
                        )
                        else feature_value
                    )
                )
                for (f_name, schema) in complex_feature_schemas.items()
            }
        else:
            _logger.debug("Fast Avro not found, using avro for deserialization.")
            return {
                f_name: (
                    lambda feature_value, avro_schema=schema: avro_schema.read(
                        BinaryDecoder(
                            BytesIO(
                                feature_value
                                if isinstance(feature_value, bytes)
                                else b64decode(feature_value)
                            )
                        )
                    )
                    # embedded features are deserialized already but not complex features stored in Opensearch
                    if (
                        isinstance(feature_value, str)
                        or isinstance(feature_value, bytes)
                    )
                    else feature_value
                )
                for (f_name, schema) in complex_feature_schemas.items()
            }

    def set_return_feature_value_handlers(
        self, features: List[tdf_mod.TrainingDatasetFeature]
    ) -> List[Callable]:
        """Build a dictionary of functions to convert/deserialize/transform the feature values returned from RonDB Server.

        Re-using the current logic from the vector server means that we currently iterate over the feature vectors
        and values multiple times, as well as converting the feature values to a dictionary and then back to a list.
        """
        self._return_feature_value_handlers = {}
        _logger.debug(
            f"Setting return feature value handlers for Feature View {self._feature_view_name},"
            f" version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        self._return_feature_value_handlers.update(
            self.build_complex_feature_decoders()
        )
        for feature in features:
            if feature.type == "timestamp":
                self._return_feature_value_handlers[feature.name] = (
                    self._handle_timestamp_based_on_dtype
                )

    def _handle_timestamp_based_on_dtype(
        self, timestamp_value: Union[str, int]
    ) -> Optional[datetime]:
        """Handle the timestamp based on the dtype which is returned.

        Currently timestamp which are in the database are returned as string. Whereas
        passed features which were given as datetime are returned as integer timestamp.

        # Arguments:
            timestamp_value: The timestamp value to be handled, either as int or str.
        """
        if timestamp_value is None:
            return None
        elif isinstance(timestamp_value, int):
            # in case of passed features, an event time could be passed as int timestamp
            return datetime.fromtimestamp(
                timestamp_value / 1000, tz=timezone.utc
            ).replace(tzinfo=None)
        elif isinstance(timestamp_value, str):
            # rest client returns timestamp as string
            return datetime.strptime(timestamp_value, self.SQL_TIMESTAMP_STRING_FORMAT)
        elif isinstance(timestamp_value, datetime):
            # sql client returns already datetime object
            return timestamp_value
        else:
            raise ValueError(
                f"Timestamp value {timestamp_value} was expected to be of type int, str or datetime."
            )

    def validate_entry(
        self,
        entry: Dict[str, Any],
        allow_missing: bool,
        passed_features: Dict[str, Any],
        vector_db_features: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Validate the provided key, value pair in the entry.

        The following checks are performed:
            - Check that all keys in the entry correspond to a serving key.
            - Check that composite keys are provided together.

        If allow_missing is set to False:
            - check that for missing serving key in entry, the feature are provided
                via passed_features or vector_db_features.

        Keys relevant to vector_db are filtered out.
        """
        _logger.debug("Checking keys in entry are valid serving keys.")
        for key in entry.keys():
            if key not in self.required_serving_keys:
                raise exceptions.FeatureStoreException(
                    f"Provided key {key} is not a serving key. Required serving keys: {self.required_serving_keys}."
                )

        _logger.debug("Checking entry has either all or none of composite serving keys")
        for composite_group in self.groups_of_composite_serving_keys.values():
            present_keys = [
                True if sk_name in entry.keys() else False
                for sk_name in composite_group
            ]
            if not all(present_keys) and any(present_keys):
                raise exceptions.FeatureStoreException(
                    "Provide either all composite serving keys or none. "
                    f"Composite keys: {composite_group} in entry {entry}."
                )

        if allow_missing is False:
            self.identify_missing_features_pre_fetch(
                entry=entry,
                passed_features=passed_features,
                vector_db_features=vector_db_features,
            )

        return {
            key: value for key, value in entry.items() if key in self.rondb_serving_keys
        }

    def identify_missing_features_pre_fetch(
        self,
        entry: Dict[str, Any],
        passed_features: Dict[str, Any],
        vector_db_features: Dict[str, Any],
    ):
        """Identify feature which will be missing in the fetched feature vector and which are not passed.

        Each serving key, value (or composite keys) need not be present in entry mapping. Missing key, value
        lead to only fetching a subset of the feature values in the query. The missing values can be filled
        via the `passed_feature` args. If `allow_missing` is set to false, then an exception should be thrown
        to match the behaviour of the sql client prior to 3.7.

        Limitation:
        - The method does not check whether serving keys correspond to existing rows in the online feature store.
        - The method does not check whether the passed features names and data types correspond to the query schema.
        """
        _logger.debug(
            "Checking missing serving keys in entry correspond to passed features."
        )
        missing_features_per_serving_keys = {}
        has_missing = False
        for sk_name, fetched_features in self.per_serving_key_features.items():
            passed_feature_names = (
                set(passed_features.keys()) if passed_features else set()
            )
            if vector_db_features and len(vector_db_features) > 0:
                _logger.debug(
                    "vector_db_features for pre-fetch missing : %s", vector_db_features
                )
                passed_feature_names = passed_feature_names.union(
                    vector_db_features.keys()
                )
            neither_fetched_nor_passed = fetched_features.difference(
                passed_feature_names
            )
            # if not present and all corresponding features are not passed via passed_features
            # or vector_db_features
            if sk_name not in entry.keys() and not fetched_features.issubset(
                passed_feature_names
            ):
                _logger.debug(
                    f"Missing serving key {sk_name} and corresponding features {neither_fetched_nor_passed}."
                )
                has_missing = True
                missing_features_per_serving_keys[sk_name] = neither_fetched_nor_passed

        if has_missing:
            raise exceptions.FeatureStoreException(
                f"Incomplete feature vector requests: {missing_features_per_serving_keys}."
                "To fetch or build a complete feature vector provide either (or both):\n"
                "\t- the missing features by adding corresponding serving key, value to the entry.\n"
                "\t- the missing features as key, value pair using the passed_features kwarg.\n"
                "Use `allow_missing=True` to allow missing features in the fetched feature vector."
            )

    def build_per_serving_key_features(
        self,
        serving_keys: List[sk_mod.ServingKey],
        features: List[tdf_mod.TrainingDatasetFeature],
    ) -> Dict[str, set[str]]:
        """Build a dictionary of feature names which will be fetched per serving key."""
        per_serving_key_features = {}
        for serving_key in serving_keys:
            per_serving_key_features[serving_key.required_serving_key] = set(
                [
                    f.name
                    for f in features
                    if f.feature_group.name == serving_key.feature_group.name
                ]
            )
        return per_serving_key_features

    @property
    def online_store_sql_client(
        self,
    ) -> Optional[online_store_sql_engine.OnlineStoreSqlClient]:
        return self._online_store_sql_client

    @property
    def online_store_rest_client_engine(
        self,
    ) -> Optional[online_store_rest_client_engine.OnlineStoreRestClientEngine]:
        return self._online_store_rest_client_engine

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
    def groups_of_composite_serving_keys(self) -> Dict[int, List[str]]:
        if not hasattr(self, "_groups_of_composite_serving_keys"):
            self._groups_of_composite_serving_keys = {
                sk.feature_group.id: [
                    sk_match.required_serving_key
                    for sk_match in self.serving_keys
                    if sk_match.feature_group.id == sk.feature_group.id
                ]
                for sk in self.serving_keys
            }
            _logger.debug(
                f"Groups of composite serving keys: {self._groups_of_composite_serving_keys}."
            )
        return self._groups_of_composite_serving_keys

    @property
    def rondb_serving_keys(self) -> List[str]:
        if not hasattr(self, "_rondb_serving_keys"):
            self._rondb_serving_keys = [
                sk.required_serving_key
                for sk in self.serving_keys
                if sk.feature_group.id not in self._skip_fg_ids
            ]
            _logger.debug(f"RonDB serving keys: {self._rondb_serving_keys}.")
        return self._rondb_serving_keys

    @property
    def training_dataset_version(self) -> Optional[int]:
        return self._training_dataset_version

    @training_dataset_version.setter
    def training_dataset_version(self, training_dataset_version: Optional[int]):
        self._training_dataset_version = training_dataset_version

    @property
    def feature_vector_col_name(self) -> List[str]:
        return self._feature_vector_col_name

    @property
    def per_serving_key_features(self) -> Dict[str, set[str]]:
        if (
            not hasattr(self, "_per_serving_key_features")
            or self._per_serving_key_features is None
            or len(self._per_serving_key_features) == 0
        ):
            self._per_serving_key_features = self.build_per_serving_key_features(
                self.serving_keys, self._features
            )
        return self._per_serving_key_features

    @property
    def transformation_functions(
        self,
    ) -> Optional[List[transformation_functions.TransformationFunction]]:
        return self._transformation_functions

    @property
    def complex_feature_decoders(self) -> Dict[str, Callable]:
        """A dictionary of functions to deserialize the complex feature values returned from RonDB Server.

        Empty if there are no complex features. Using the Avro Schema to deserialize the complex feature values.
        """
        return self._complex_feature_decoders

    @property
    def return_feature_value_handlers(self) -> Dict[str, Callable]:
        """A dictionary of functions to the feature values returned from RonDB Server."""
        return self._return_feature_value_handlers

    @property
    def transformation_function_engine(
        self,
    ) -> tf_engine_mod.TransformationFunctionEngine:
        return self._transformation_function_engine

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

    @property
    def transformed_feature_vector_col_name(self):
        if self._transformed_feature_vector_col_name is None:
            transformation_features = []
            output_column_names = []
            for transformation_function in self._transformation_functions:
                transformation_features += (
                    transformation_function.hopsworks_udf.transformation_features
                )
                output_column_names += (
                    transformation_function.hopsworks_udf.output_column_names
                )

            self._transformed_feature_vector_col_name = [
                feature
                for feature in self._feature_vector_col_name
                if feature not in transformation_features
            ]
            self._transformed_feature_vector_col_name.extend(output_column_names)
        return self._transformed_feature_vector_col_name
