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

from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from hsfs import training_dataset
from hsfs.core import transformation_function_api


if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyspark.sql as ps
    from hsfs.feature_view import FeatureView
    from hsfs.statistics import Statistics
    from hsfs.transformation_function import TransformationFunction

from hsfs import (
    feature_view,
    statistics,
    training_dataset,
    training_dataset_feature,
    transformation_function_attached,
    util,
)
from hsfs.core import (
    feature_view_api,
    statistics_api,
    statistics_engine,
    transformation_function_api,
)
from hsfs.core.builtin_transformation_function import BuiltInTransformationFunction

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
        self, transformation_fn_instance: TransformationFunction
    ) -> TransformationFunction:
        """
        Save a transformation function into the feature store.

        # Argument
            transformation_fn_instance `TransformationFunction`: The transformation function to be saved into the feature store.
        """
        self._transformation_function_api.register_transformation_fn(
            transformation_fn_instance
        )

    def get_transformation_fn(
        self, name: str, version: Optional[int] = None
    ) -> Union[TransformationFunction, List[TransformationFunction]]:
        """
        Retrieve a transformation function from the feature store.

        If only the name of the transformation function is provided then all the versions of the transformation functions are returned as a list.
        If both name and version are not provided then all transformation functions saved in the feature view is returned.

        # Argument
            name ` Optional[str]`: The name of the transformation function to be retrieved.
            version `Optional[int]`: The version of the transformation function to be retrieved.
        # Returns
            `Union[TransformationFunction, List[TransformationFunction]]` : A transformation function if name and version is provided. A list of transformation functions if only name is provided.
        """

        transformation_fn_instances = (
            self._transformation_function_api.get_transformation_fn(name, version)
        )
        return transformation_fn_instances

    def get_transformation_fns(self) -> List[TransformationFunction]:
        """
        Get all the transformation functions in the feature store

        # Returns
            `List[TransformationFunction]` : A list of transformation functions.
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

    def delete(self, transformation_function_instance: TransformationFunction) -> None:
        """
        Delete a transformation function from the feature store.

        # Arguments
            transformation_function_instance `TransformationFunction`: The transformation function to be removed from the feature store.
        """
        self._transformation_function_api.delete(transformation_function_instance)

    @staticmethod
    def compute_transformation_fn_statistics(
        training_dataset_obj: training_dataset.TrainingDataset,
        statistics_features: List[str],
        label_encoder_features: List[str],
        feature_dataframe: Union[pd.DataFrame, pl.DataFrame, ps.DataFrame],
        feature_view_obj: FeatureView,
    ) -> Statistics:
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
    def compute_and_set_feature_statistics(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: FeatureView,
        dataset: Union[
            Dict[str, Union[pd.DataFrame, pl.DataFrame, ps.DataFrame]],
            Union[pd.DataFrame, pl.DataFrame, ps.DataFrame],
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

        # Finding the features for which statistics is required
        for transformation_function in feature_view_obj.transformation_functions:
            statistics_features.update(
                transformation_function.hopsworks_udf.statistics_features
            )
        if statistics_features:
            # compute statistics on training data
            if training_dataset.splits:
                # compute statistics before transformations are applied
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        list(statistics_features),
                        [],
                        dataset.get(training_dataset.train_split),
                        feature_view_obj,
                    )
                )
            else:
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        list(statistics_features),
                        [],
                        dataset,
                        feature_view_obj,
                    )
                )

            # Set statistics computed in the hopsworks UDF
            for transformation_function in feature_view_obj.transformation_functions:
                transformation_function.hopsworks_udf.transformation_statistics = (
                    stats.feature_descriptive_statistics
                )

    @staticmethod
    def get_and_set_feature_statistics(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: FeatureView,
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

            for transformation_function in feature_view_obj.transformation_functions:
                transformation_function.hopsworks_udf.transformation_statistics = (
                    td_tffn_stats.feature_descriptive_statistics
                )
