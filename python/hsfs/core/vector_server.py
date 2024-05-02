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
from base64 import b64decode
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional, Set, Union

import avro.io
import hsfs
import hsfs.client
import hsfs.training_dataset_feature
import numpy as np
import pandas as pd
import polars as pl
from avro.schema import Schema
from hsfs import (
    client,
    feature_view,
    training_dataset,
)
from hsfs import (
    transformation_function_attached as tfa_mod,
)
from hsfs.core import (
    online_store_rest_client_engine,
    online_store_sql_client,
)
from hsfs.core import (
    transformation_function_engine as tfe_mod,
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

    def __init__(
        self,
        feature_store_id: int,
        features: Optional[
            List[hsfs.training_dataset_feature.TrainingDatasetFeature]
        ] = None,
        training_dataset_version: Optional[int] = None,
        serving_keys: Optional[List[hsfs.serving_key.ServingKey]] = None,
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

        self._skip_fg_ids = skip_fg_ids or set()
        self._serving_keys = serving_keys or []
        self._required_serving_keys = []

        self._transformation_function_engine = tfe_mod.TransformationFunctionEngine(
            feature_store_id
        )
        self._transformation_functions = None
        self._online_store_sql_client = None

        self._online_store_rest_client_engine = None
        self._init_online_store_rest_client: Optional[bool] = None
        self._init_online_store_sql_client: Optional[bool] = None
        self._default_online_store_client = None

        self.set_return_feature_value_handlers(features=self._features)

    def init_serving(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
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
        self._complex_features = self.get_complex_feature_schemas()

        if len(self._complex_features) > 0:
            _logger.debug(f"Complex feature schemas: {self._complex_features}")
            self._complex_feature_decoders = self.build_complex_feature_decoders(
                complex_feature_schemas=self._complex_features
            )
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
        _logger.info("Initialising Vector Server Online Store SQL client")
        self._online_store_sql_client = online_store_sql_client.OnlineStoreSqlClient(
            feature_store_id=self._feature_store_id,
            skip_fg_ids=self._skip_fg_ids,
            serving_keys=self.serving_keys,
        )
        self.online_store_sql_client.init_prepared_statements(
            entity,
            external,
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

        client.online_store_rest_client.init_or_reset_online_store_rest_client(
            optional_config=online_store_rest_client_config,
            reset_client=reset_online_rest_client,
        )

    def get_feature_vector(
        self,
        entry: Dict[str, Any],
        return_type: Optional[str] = None,
        passed_features: Optional[Dict[str, Any]] = None,
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, np.ndarray, List[Any], Dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        if passed_features is None:
            passed_features = {}

        online_client_choice = self.which_client_and_ensure_initialised(
            force_rest_client=force_rest_client, force_sql_client=force_sql_client
        )

        if online_client_choice == self.DEFAULT_ONLINE_STORE_REST_CLIENT:
            _logger.info("get_feature_vector Online REST client")
            serving_vector = self.online_store_rest_client_engine.get_single_feature_vector(
                entry,
                return_type=self.online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
        else:
            # get result row
            _logger.info("get_feature_vector Online SQL client")
            serving_vector = self.online_store_sql_client.get_single_feature_vector(
                entry
            )

        # Deserialize complex features
        _logger.debug("Deserializing complex features")
        serving_vector = self.deserialize_complex_features(serving_vector)

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
        passed_features: List[Dict[str, Any]] = None,
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, np.ndarray, List[Any], List[Dict[str, Any]]]:
        """Assembles serving vector from online feature store."""
        if passed_features is None:
            passed_features = []

        online_client_choice = self.which_client_and_ensure_initialised(
            force_rest_client=force_rest_client, force_sql_client=force_sql_client
        )
        if online_client_choice == self.DEFAULT_ONLINE_STORE_REST_CLIENT:
            _logger.info("get_batch_feature_vector Online REST client")
            batch_results = self.online_store_rest_client_engine.get_batch_feature_vectors(
                entries=entries,
                return_type=self.online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
        else:
            # get result row
            _logger.info("get_batch_feature_vectors through SQL client")
            batch_results, _ = self.online_store_sql_client.get_batch_feature_vectors(
                entries
            )

        # Deserialize complex features
        _logger.debug("Deserializing complex features")
        batch_results = list(
            map(
                lambda row_dict: self.deserialize_complex_features(row_dict),
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
        feature_vectorz: Union[List[Any], List[List[Any]]],
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
        if return_type.lower() == "list" and not inference_helper:
            _logger.debug("Returning feature vector as value list")
            return feature_vectorz
        elif return_type.lower() == "dict":
            _logger.debug("Returning feature vector as dictionary")
            return {
                self._feature_vector_col_name[i]: feature_vectorz[i]
                for i in range(len(self._feature_vector_col_name))
            }
        elif return_type.lower() == "numpy":
            _logger.debug("Returning feature vector as numpy array")
            return np.array(feature_vectorz)
        elif return_type.lower() == "pandas":
            _logger.debug("Returning feature vector as pandas dataframe")
            if batch:
                pandas_df = pd.DataFrame(feature_vectorz)
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
                f"""Unknown return type. Supported return types are {"'list', " if not inference_helper else ""}'dict', 'polars', 'pandas' and 'numpy'"""
            )

    def get_inference_helper(
        self, entry: Dict[str, Any], return_type: str
    ) -> Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        _logger.info("Retrieve inference helper values for single entry.")
        _logger.debug(f"entry: {entry} as return type: {return_type}")
        return self.handle_feature_vector_return_type(
            self.online_store_sql_client.get_inference_helper_vector(entry),
            batch=False,
            inference_helper=True,
            return_type=return_type,
        )

    def get_inference_helpers(
        self,
        feature_view_object: "feature_view.FeatureView",
        entries: List[Dict[str, Any]],
        return_type: str,
    ) -> Union[pd.DataFrame, pl.DataFrame, List[Dict[str, Any]]]:
        """Assembles serving vector from online feature store."""
        batch_results = self.online_store_sql_client.get_batch_inference_helper_vectors(
            entries
        )

        # drop serving and primary key names from the result dict
        drop_list = self.serving_keys + list(feature_view_object.primary_keys)

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

    def build_complex_feature_decoders(
        self, complex_feature_schemas: Dict[str, Schema]
    ) -> Dict[str, Callable]:
        """Build a dictionary of functions to deserialize the complex feature values returned from RonDB Server."""
        if HAS_FASTAVRO:
            return {
                f_name: (
                    lambda feature_value, avro_schema=schema: schemaless_reader(
                        BytesIO(b64decode(feature_value)),
                        avro_schema.writers_schema.to_json(),
                    )
                    # embedded features are deserialized already but not complex features stored in Opensearch
                    if isinstance(feature_value, str)
                    else feature_value
                )
                for (f_name, schema) in complex_feature_schemas.items()
            }
        else:
            return {
                f_name: lambda feature_value, avro_schema=schema: avro_schema.read(
                    BinaryDecoder(BytesIO(b64decode(feature_value)))
                )
                for (f_name, schema) in complex_feature_schemas.items()
            }

    def _handle_timestamp_based_on_dtype(
        self, timestamp_value: Union[str, int]
    ) -> Optional[datetime]:
        """Handle the timestamp based on the dtype which is returned.

        Currently timestamp which are in the database are returned as string. Whereas
        passed features which were given as datetime are returned as integer timestamp.

        # Arguments:
            timestamp_value: The timestamp value to be handled, either as int or str.
        """
        if isinstance(timestamp_value, int):
            _logger.debug(
                f"Converting timestamp {timestamp_value} to datetime from UNIX timestamp."
            )
            return datetime.fromtimestamp(
                timestamp_value / 1000, tz=timezone.utc
            ).replace(tzinfo=None)
        elif isinstance(timestamp_value, str):
            _logger.debug(
                f"Parsing timestamp {timestamp_value} to datetime from SQL timestamp string."
            )
            return datetime.strptime(timestamp_value, self.SQL_TIMESTAMP_STRING_FORMAT)
        elif isinstance(timestamp_value, datetime):
            _logger.debug(
                f"Returning passed timestamp {timestamp_value} as it is already a datetime."
            )
            return timestamp_value
        else:
            raise ValueError(
                f"Timestamp value {timestamp_value} was expected to be of type int or str."
            )

    def set_return_feature_value_handlers(
        self, features: List[hsfs.training_dataset_feature.TrainingDatasetFeature]
    ) -> List[Callable]:
        """Build a dictionary of functions to convert/deserialize/transform the feature values returned from RonDB Server.

        TODO: Trim down logging once stabilised

        Re-using the current logic from the vector server means that we currently iterate over the feature vectors
        and values multiple times, as well as converting the feature values to a dictionary and then back to a list.

        - The handlers do not need to handle the case where the feature value is None.
        - The handlers should first convert/deserialize the feature values and then transform them if necessary.
        - The handlers should be reusable without paying overhead for rebuilding them for each feature vector.
        """
        self._return_feature_value_handlers = {}
        self._ordered_feature_names = []
        self._ordered_feature_group_feature_names = []
        _logger.debug(
            f"Setting return feature value handlers for Feature View {self._feature_view_name},"
            f" version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
        )
        for feature in features:
            # These features are not part of the feature vector.
            if feature.label or feature.training_helper_column:
                _logger.debug(
                    f"Skipping Feature {feature.name} as it is a label or training helper column."
                )
                continue

            self._ordered_feature_names.append(feature.name)
            self._ordered_feature_group_feature_names.append(
                feature.feature_group_feature_name
            )
            if (
                feature.is_complex()
                and feature.name in self.transformation_functions.keys()
            ):
                # deserialize and then transform
                _logger.debug(
                    f"Adding return feature value deserializer for complex feature {feature.name} with transformation function."
                )
                self._return_value_handlers[feature.name] = (
                    lambda feature_value, feature_name=feature.name: _logger.debug(
                        f"Deserialize and transform value of feature {feature_name}"
                    )
                    or self.transformation_fns[feature_name].transformation_fn(
                        self._complex_feature_decoder[feature_name](feature_value)
                    )
                )
            elif feature.is_complex():
                # deserialize only
                _logger.debug(
                    f"Adding return feature value deserializer for complex feature {feature.name}."
                )
                self._return_feature_value_handlers[feature.name] = (
                    lambda feature_value, feature_name=feature.name: _logger.debug(
                        f"Transform value of feature {feature_name}"
                    )
                    or self._complex_feature_decoders[feature_name](feature_value)
                )
            elif (
                feature.type == "timestamp"
                and feature.name in self.transformation_functions.keys()
            ):
                # convert and then transform
                _logger.debug(
                    f"Adding return feature value converter for timestamp feature {feature.name} with transformation function."
                )
                self._return_feature_value_handlers[feature.name] = (
                    lambda feature_value, feature_name=feature.name: _logger.debug(
                        f"Convert and transform timestamp feature {feature_name}"
                    )
                    or self._transformation_functions[feature_name].transformation_fn(
                        self._handle_timestamp_based_on_dtype(feature_value)
                    )
                )
            elif feature.type == "timestamp":
                # convert only
                _logger.debug(
                    f"Adding return feature value converter for timestamp feature {feature.name}."
                )
                self._return_feature_value_handlers[feature.name] = (
                    self._handle_timestamp_based_on_dtype
                )
            elif feature.name in self.transformation_functions.keys():
                # transform only
                _logger.debug(
                    f"Adding return feature value transformation for feature {feature.name}."
                )
                self._return_feature_value_handlers[feature.name] = (
                    lambda feature_value, feature_name=feature.name: _logger.debug(
                        f"Transform value of feature {feature_name}"
                    )
                    or self.transformation_functions[feature_name].transformation_fn(
                        feature_value
                    )
                )
            else:
                # no transformation
                _logger.debug(
                    f"Adding return feature value handler for feature {feature.name}."
                )
                self._return_feature_value_handlers[feature.name] = (
                    lambda feature_value, feature_name=feature.name: _logger.debug(
                        f"Applying identity to value of {feature_name}"
                    )
                    or feature_value
                )
        _logger.debug(
            f"Ordered feature names, skipping label or training helper columns: {self._ordered_feature_names}"
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

    def deserialize_complex_features(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        for feature_name, schema in self._complex_features.items():
            if feature_name in row_dict:
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

    def _apply_transformation(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        for feature_name in self._transformation_functions:
            if feature_name in row_dict:
                transformation_fn = self._transformation_functions[
                    feature_name
                ].transformation_fn
                row_dict[feature_name] = transformation_fn(row_dict[feature_name])
        return row_dict

    @property
    def online_store_sql_client(
        self,
    ) -> Optional[online_store_sql_client.OnlineStoreSqlClient]:
        return self._online_store_sql_client

    @property
    def online_store_rest_client_engine(
        self,
    ) -> Optional[online_store_rest_client_engine.OnlineStoreRestClientEngine]:
        return self._online_store_rest_client_engine

    @property
    def serving_keys(self) -> List[hsfs.serving_key.ServingKey]:
        return self._serving_keys

    @serving_keys.setter
    def serving_keys(self, serving_vector_keys: List[hsfs.serving_key.ServingKey]):
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
    def ordered_feature_names(self) -> List[str]:
        return self._ordered_feature_names

    @property
    def transformation_functions(
        self,
    ) -> Dict[str, tfa_mod.TransformationFunctionAttached]:
        if self._transformation_functions is None:
            self._transformation_functions = {}
        return self._transformation_functions

    @property
    def complex_feature_decoders(self) -> Dict[str, Callable]:
        """A dictionary of functions to deserialize the complex feature values returned from RonDB Server.

        Empty if there are no complex features. Using the Avro Schema to deserialize the complex feature values.
        """
        return self._complex_feature_decoders

    @property
    def transformation_function_engine(
        self,
    ) -> transformation_function_engine.TransformationFunctionEngine:
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
