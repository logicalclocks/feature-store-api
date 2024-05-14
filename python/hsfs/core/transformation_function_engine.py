#
#   Copyright 2021 Logical Clocks AB
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

from typing import Dict, List, Optional, Set, TypeVar, Union

import pandas as pd
import polars as pl
from hsfs import feature_view, statistics, training_dataset, transformation_function
from hsfs.core import transformation_function_api


class TransformationFunctionEngine:
    BUILTIN_FN_NAMES = [
        "min_max_scaler",
        "standard_scaler",
        "robust_scaler",
        "label_encoder",
    ]
    AMBIGUOUS_FEATURE_ERROR = (
        "Provided feature '{}' in transformation functions is ambiguous and exists in more than one feature groups."
        "You can provide the feature with the prefix that was specified in the join."
    )
    FEATURE_NOT_EXIST_ERROR = "Provided feature '{}' in transformation functions do not exist in any of the feature groups."

    def __init__(self, feature_store_id: int):
        self._feature_store_id = feature_store_id
        self._transformation_function_api: transformation_function_api.TransformationFunctionApi = transformation_function_api.TransformationFunctionApi(
            feature_store_id
        )

    def save(
        self, transformation_fn_instance: transformation_function.TransformationFunction
    ) -> transformation_function.TransformationFunction:
        """
        Save a transformation function into the feature store.

        # Argument
            transformation_fn_instance `transformation_function.TransformationFunction`: The transformation function to be saved into the feature store.
        """
        self._transformation_function_api.register_transformation_fn(
            transformation_fn_instance
        )

    def get_transformation_fn(
        self, name: str, version: Optional[int] = None
    ) -> Union[
        transformation_function.TransformationFunction,
        List[transformation_function.TransformationFunction],
    ]:
        """
        Retrieve a transformation function from the feature store.

        If only the name of the transformation function is provided then all the versions of the transformation functions are returned as a list.
        If both name and version are not provided then all transformation functions saved in the feature view is returned.

        # Argument
            name ` Optional[str]`: The name of the transformation function to be retrieved.
            version `Optional[int]`: The version of the transformation function to be retrieved.
        # Returns
            `Union[transformation_function.TransformationFunction, List[transformation_function.TransformationFunction]]` : A transformation function if name and version is provided. A list of transformation functions if only name is provided.
        """

        transformation_fn_instances = (
            self._transformation_function_api.get_transformation_fn(name, version)
        )
        return transformation_fn_instances

    def get_transformation_fns(
        self,
    ) -> List[transformation_function.TransformationFunction]:
        """
        Get all the transformation functions in the feature store

        # Returns
            `List[transformation_function.TransformationFunction]` : A list of transformation functions.
        """
        transformation_fn_instances = (
            self._transformation_function_api.get_transformation_fn(
                name=None, version=None
            )
        )
        transformation_fns = []
        for (
            transformation_fn_instance
        ) in transformation_fn_instances:  # todo what is the point of this?
            transformation_fns.append(transformation_fn_instance)
        return transformation_fns

    def delete(
        self,
        transformation_function_instance: transformation_function.TransformationFunction,
    ) -> None:
        """
        Delete a transformation function from the feature store.

        # Arguments
            transformation_function_instance `transformation_function.TransformationFunction`: The transformation function to be removed from the feature store.
        """
        self._transformation_function_api.delete(transformation_function_instance)

    @staticmethod
    def compute_transformation_fn_statistics(
        training_dataset_obj: training_dataset.TrainingDataset,
        statistics_features: List[str],
        label_encoder_features: List[str],
        feature_dataframe: Union[
            pd.DataFrame, pl.DataFrame, TypeVar("pyspark.sql.DataFrame")
        ],
        feature_view_obj: feature_view.FeatureView,
    ) -> statistics.Statistics:
        """
        Compute the statistics required for a training dataset object.

        # Arguments
            training_dataset_obj `TrainingDataset`: The training dataset for which the statistics is to be computed.
            statistics_features `List[str]`: The list of features for which the statistics should be computed.
            label_encoder_features `List[str]`: Features used for label encoding.
            feature_dataframe `Union[pd.DataFrame, pl.DataFrame, ps.DataFrame]`: The dataframe that contains the data for which the statistics must be computed.
            feature_view_obj `FeatureView`: The feature view in which the training data is being created.
        # Returns
            `Statistics` : The statistics object that contains the statistics for each features.
        """
        return training_dataset_obj._statistics_engine.compute_transformation_fn_statistics(
            td_metadata_instance=training_dataset_obj,
            columns=statistics_features,
            label_encoder_features=label_encoder_features,  # label encoded features only
            feature_dataframe=feature_dataframe,
            feature_view_obj=feature_view_obj,
        )

    @staticmethod
    def get_ready_to_use_transformation_fns(
        feature_view: feature_view.FeatureView,
        training_dataset_version: Optional[int] = None,
    ) -> List[transformation_function.TransformationFunction]:
        # get attached transformation functions
        transformation_functions = (
            feature_view._feature_view_engine.get_attached_transformation_fn(
                feature_view.name, feature_view.version
            )
        )

        transformation_functions = (
            [transformation_functions]
            if not isinstance(transformation_functions, list)
            else transformation_functions
        )

        is_stat_required = any(
            [tf.hopsworks_udf.statistics_required for tf in transformation_functions]
        )
        if not is_stat_required:
            td_tffn_stats = None
        else:
            # if there are any transformation functions that require statistics get related statistics and
            # populate with relevant arguments
            # there should be only one statistics object with before_transformation=true
            if training_dataset_version is None:
                raise ValueError(
                    "Training data version is required for transformation. Call `feature_view.init_serving(version)` "
                    "or `feature_view.init_batch_scoring(version)` to pass the training dataset version."
                    "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
                )
            td_tffn_stats = feature_view._statistics_engine.get(
                feature_view,
                before_transformation=True,
                training_dataset_version=training_dataset_version,
            )

        if is_stat_required and td_tffn_stats is None:
            raise ValueError(
                "No statistics available for initializing transformation functions."
                + "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
            )

        if is_stat_required:
            for transformation_function in transformation_functions:
                transformation_function.hopsworks_udf.transformation_statistics = (
                    td_tffn_stats.feature_descriptive_statistics
                )
        return transformation_functions

    @staticmethod
    def compute_and_set_feature_statistics(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        dataset: Union[
            Dict[
                str, Union[pd.DataFrame, pl.DataFrame, TypeVar("pyspark.sql.DataFrame")]
            ],
            Union[pd.DataFrame, pl.DataFrame, TypeVar("pyspark.sql.DataFrame")],
        ],
    ) -> None:
        """
        Function that computes and sets the statistics required for the UDF used for transformation.

        The function assigns the statistics computed to hopsworks UDF object so that the statistics can be used when UDF is executed.

        # Argument
            training_dataset_obj `TrainingDataset`: The training dataset for which the statistics is to be computed.
            feature_view `FeatureView`: The feature view in which the training data is being created.
            dataset `Union[Dict[str,  Union[pd.DataFrame, pl.DataFrame, ps.DataFrame]],  Union[pd.DataFrame, pl.DataFrame, ps.DataFrame]]`: A dataframe that conqtains the training data or a dictionary that contains both the training and test data.
        """
        statistics_features: Set[str] = set()
        label_encoder_features: Set[str] = set()

        # Finding the features for which statistics is required
        for tf in feature_view_obj.transformation_functions:
            statistics_features.update(tf.hopsworks_udf.statistics_features)
            if (
                tf.hopsworks_udf.function_name == "label_encoder"
                or tf.hopsworks_udf.function_name == "one_hot_encoder"
            ):
                label_encoder_features.update(tf.hopsworks_udf.statistics_features)
        if statistics_features:
            # compute statistics on training data
            if training_dataset.splits:
                # compute statistics before transformations are applied
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        list(statistics_features),
                        list(label_encoder_features),
                        dataset.get(training_dataset.train_split),
                        feature_view_obj,
                    )
                )
            else:
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        list(statistics_features),
                        list(label_encoder_features),
                        dataset,
                        feature_view_obj,
                    )
                )

            # Set statistics computed in the hopsworks UDF
            for tf in feature_view_obj.transformation_functions:
                tf.hopsworks_udf.transformation_statistics = (
                    stats.feature_descriptive_statistics
                )

    @staticmethod
    def get_and_set_feature_statistics(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        training_dataset_version: int = None,
    ) -> None:
        """
        Function that gets the transformation statistics computed while creating the training dataset from the backend and assigns it to the hopsworks UDF object.

        The function assigns the statistics computed to hopsworks UDF object so that the statistics can be used when UDF is executed.

        # Argument
            training_dataset_obj `TrainingDataset`: The training dataset for which the statistics is to be computed.
            feature_view `FeatureView`: The feature view in which the training data is being created.
            training_dataset_version `int`: The version of the training dataset for which the statistics is to be retrieved.

        # Raises
            `ValueError` : If the statistics are not present in the backend.
        """

        is_stat_required = any(
            [
                tf.hopsworks_udf.statistics_required
                for tf in feature_view_obj.transformation_functions
            ]
        )

        if is_stat_required:
            td_tffn_stats = training_dataset._statistics_engine.get(
                feature_view_obj,
                before_transformation=True,
                training_dataset_version=training_dataset_version,
            )

            if td_tffn_stats is None:
                raise ValueError(
                    "No statistics available for initializing transformation functions."
                )

            for tf in feature_view_obj.transformation_functions:
                tf.hopsworks_udf.transformation_statistics = (
                    td_tffn_stats.feature_descriptive_statistics
                )
