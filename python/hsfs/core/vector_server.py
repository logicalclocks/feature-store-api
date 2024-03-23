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
import io
import logging
from typing import Any, Dict, List, Optional, Union

import avro.io
import avro.schema
import hsfs
import numpy as np
import pandas as pd
import polars as pl
from hsfs import client, feature_view, training_dataset
from hsfs.core import (
    feature_view_engine,
    online_store_rest_client_engine,
    online_store_sql_client,
    transformation_function_engine,
)


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
            List["hsfs.training_dataset_feature.TrainingDatasetFeature"]
        ] = None,
        training_dataset_version: Optional[int] = None,
        serving_keys: Optional[List["hsfs.serving_key.ServingKey"]] = None,
        skip_fg_ids=None,
        feature_store_name: str = None,
        feature_view_name: str = None,
        feature_view_version: int = None,
    ):
        self._training_dataset_version = training_dataset_version

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
        self._serving_keys = serving_keys

        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

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

        self._online_store_sql_client = None
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
        options: Optional[Dict[str, Any]] = None,
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
            self.setup_online_store_rest_client_and_engine(
                entity=entity, options=options
            )

        if self._init_online_store_sql_client is True:
            self.setup_online_store_sql_client(
                entity=entity,
                batch=batch,
                external=external,
                inference_helper_columns=inference_helper_columns,
                options=options,
            )

    def init_batch_scoring(
        self,
        entity: Union["feature_view.FeatureView", "training_dataset.TrainingDataset"],
    ):
        self.init_transformation(entity)

    def init_transformation(
        self,
        entity: Union["feature_view.FeatureView", "training_dataset.TrainingDataset"],
    ):
        # attach transformation functions
        self._transformation_functions = self._get_transformation_fns(entity)

    def setup_online_store_rest_client_and_engine(
        self,
        entity: Union["feature_view.FeatureView", "training_dataset.TrainingDataset"],
        options: Optional[Dict[str, Any]] = None,
    ):
        # naming is off here, but it avoids confusion with the argument init_online_store_rest_client
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

    def setup_online_store_sql_client(
        self,
        entity: Union["feature_view.FeatureView", "training_dataset.TrainingDataset"],
        batch: bool,
        external: bool,
        inference_helper_columns: bool,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        _logger.info("Initialising Vector Server Online SQL client")
        self._online_store_sql_client = online_store_sql_client.OnlineStoreSqlClient(
            feature_store_id=self._feature_store_id,
        )
        self.online_store_sql_client.init_prepared_statement(
            entity,
            batch,
            external,
            inference_helper_columns,
            options=options,
        )

    def get_feature_vector(
        self,
        entry: Dict[str, Any],
        return_type: Optional[str] = None,
        passed_features: Optional[Dict[str, Any]] = None,
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, List[Any], Dict[str, Any]]:
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
            batch_results, _ = self._batch_vector_results(entries)
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
    ) -> Union[pd.DataFrame, pl.DataFrame, List[Any], Dict[str, Any]]:
        if return_type.lower() == "list" and not inference_helper:
            _logger.debug("Returning feature vector as value list")
            return feature_vectorz
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
            polars_df = pl.DataFrame(
                feature_vectorz if batch else [feature_vectorz],
                schema=self._feature_vector_col_name if not inference_helper else None,
                orient="row",
            )
            return polars_df
        else:
            raise ValueError(
                f"""Unknown return type. Supported return types are {"'list', " if not inference_helper else ""}'polars', 'pandas' and 'numpy'"""
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

    def get_inference_helper(
        self, entry, return_type: str
    ) -> Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        _logger.info("Retrieve inference helper values for single entry.")
        _logger.debug(f"entry: {entry} as return type: {return_type}")
        return self.handle_feature_vector_return_type(
            self.get_inference_helper_result(entry),
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

    def deserialize_complex_features(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        for feature_name, schema in self._complex_features.items():
            if feature_name in row_dict:
                bytes_reader = io.BytesIO(row_dict[feature_name])
                decoder = avro.io.BinaryDecoder(bytes_reader)
                row_dict[feature_name] = schema.read(decoder)
        return row_dict

    def _generate_vector(self, result_dict: Dict[str, Any], fill_na=False):
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
                    "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
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

    @property
    def online_store_sql_client(self) -> "online_store_sql_client.OnlineStoreSqlClient":
        return self._online_store_sql_client

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
                f"Default Online Store Cient is set to {self.DEFAULT_ONLINE_STORE_REST_CLIENT} but Online Store REST client"
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
