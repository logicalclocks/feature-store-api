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
import hsfs
import numpy as np
import pandas as pd
import polars as pl
from hsfs import (
    client,
    feature_view,
    training_dataset,
    transformation_function_attached,
)
from hsfs.core import (
    online_store_sql_client,
    transformation_function_engine,
)


_logger = logging.getLogger(__name__)


class VectorServer:
    def __init__(
        self,
        feature_store_id: int,
        features: Optional[
            List[hsfs.training_dataset_feature.TrainingDatasetFeature]
        ] = None,
        training_dataset_version: Optional[int] = None,
        serving_keys: Optional[List[hsfs.serving_key.ServingKey]] = None,
        skip_fg_ids: Optional[Set] = None,
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

        self._skip_fg_ids = skip_fg_ids or set()
        self._serving_keys = serving_keys

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                feature_store_id
            )
        )
        self._transformation_functions = None
        self._required_serving_keys = None
        self._online_store_sql_client = None

    def init_serving(
        self,
        entity: Union[feature_view.FeatureView, training_dataset.TrainingDataset],
        external: bool = False,
        inference_helper_columns: bool = False,
        options: Optional[Dict[str, Any]] = None,
    ):
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
        self._transformation_functions = (
            self.transformation_function_engine.get_ready_to_use_transformation_fns(
                entity
            )
        )

    def setup_online_store_sql_client(
        self,
        entity: Union["feature_view.FeatureView", "training_dataset.TrainingDataset"],
        external: bool,
        inference_helper_columns: bool,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        _logger.info("Initialising Vector Server Online SQL client")
        self._online_store_sql_client = online_store_sql_client.OnlineStoreSqlClient(
            feature_store_id=self._feature_store_id,
            skip_fg_ids=self._skip_fg_ids,
        )
        self.online_store_sql_client.init_prepared_statements(
            entity,
            external,
            inference_helper_columns,
        )
        self.online_store_sql_client.init_async_mysql_connection(options=options)

    def get_feature_vector(
        self,
        entry: Dict[str, Any],
        return_type: Optional[str] = None,
        passed_features: Optional[Dict[str, Any]] = None,
        allow_missing: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, np.ndarray, List[Any], Dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        if passed_features is None:
            passed_features = {}

        # get result row
        _logger.info("get_feature_vector Online SQL client")
        serving_vector = self.online_store_sql_client.get_single_feature_vector(entry)
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
    ) -> Union[pd.DataFrame, pl.DataFrame, np.ndarray, List[Any], List[Dict[str, Any]]]:
        if passed_features is None:
            passed_features = []
        """Assembles serving vector from online feature store."""
        _logger.info("get_feature_vectors through SQL client")
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
    def required_serving_keys(self) -> Set[str]:
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
    def online_store_sql_client(
        self,
    ) -> Optional["online_store_sql_client.OnlineStoreSqlClient"]:
        return self._online_store_sql_client

    @property
    def serving_keys(self) -> List["hsfs.serving_key.ServingKey"]:
        return self._serving_keys

    @serving_keys.setter
    def serving_keys(self, serving_vector_keys: List["hsfs.serving_key.ServingKey"]):
        self._serving_keys = serving_vector_keys

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
        Dict[str, "transformation_function_attached.TransformationFunctionAttached"]
    ]:
        return self._transformation_functions

    @property
    def transformation_function_engine(
        self,
    ) -> "transformation_function_engine.TransformationFunctionEngine":
        return self._transformation_function_engine
