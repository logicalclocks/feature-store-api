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

import datetime
from functools import partial
from typing import Dict, Optional, Union

import hsfs
import numpy
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
        self._transformation_function_api = (
            transformation_function_api.TransformationFunctionApi(feature_store_id)
        )
        self._statistics_api = statistics_api.StatisticsApi(
            feature_store_id, training_dataset.TrainingDataset.ENTITY_TYPE
        )
        self._feature_view_api: Optional["feature_view_api.FeatureViewApi"] = None
        self._statistics_engine: Optional["statistics_engine.StatisticsEngine"] = None

    def save(self, transformation_fn_instance):
        if self.is_builtin(transformation_fn_instance):
            raise ValueError(
                "Transformation function name '{name:}' with version 1 is reserved for built-in hsfs "
                "functions. Please use other name or version".format(
                    name=transformation_fn_instance.name
                )
            )
        if not callable(transformation_fn_instance.transformation_fn):
            raise ValueError("transformer must be callable")
        self._transformation_function_api.register_transformation_fn(
            transformation_fn_instance
        )

    def get_transformation_fn(self, name, version=None):
        transformation_fn_instances = (
            self._transformation_function_api.get_transformation_fn(name, version)
        )
        return transformation_fn_instances[0]

    def get_transformation_fns(self):
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

    def delete(self, transformation_function_instance):
        self._transformation_function_api.delete(transformation_function_instance)

    def get_td_transformation_fn(self, training_dataset):
        attached_transformation_fns = (
            self._transformation_function_api.get_td_transformation_fn(training_dataset)
        )
        transformation_fn_dict = {}
        for attached_transformation_fn in attached_transformation_fns:
            transformation_fn_dict[attached_transformation_fn.name] = (
                attached_transformation_fn.transformation_function
            )
        return transformation_fn_dict

    @staticmethod
    def attach_transformation_fn(training_dataset_obj=None, feature_view_obj=None):
        # TODO : Remove transformation function attached to training dataset object and features
        if training_dataset_obj:
            target_obj = training_dataset_obj  # todo why provide td and fv just to convert to target_obj?
        else:
            target_obj = feature_view_obj

        if target_obj._transformation_functions:
            for (
                feature_name,
                transformation_fn,
            ) in target_obj._transformation_functions.items():
                if feature_name in target_obj.labels:
                    raise ValueError(
                        "Online transformations for training dataset labels are not supported."
                    )

                feature, prefix, featuregroup = target_obj.query._get_feature_by_name(
                    feature_name
                )
                target_obj._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature_name,
                        feature_group_feature_name=feature.name,
                        featuregroup=featuregroup,
                        type=transformation_fn.output_type,
                        label=False,
                        transformation_function=transformation_fn,
                    )
                )

    def is_builtin(self, transformation_fn_instance):
        return (
            transformation_fn_instance.name in self.BUILTIN_FN_NAMES
            and transformation_fn_instance.version == 1
        )

    @staticmethod
    def populate_builtin_fn_arguments(
        feature_name, transformation_function_instance, feature_descriptive_stats
    ):
        # TODO : Make this statistics
        if transformation_function_instance.name == "min_max_scaler":
            min_value, max_value = BuiltInTransformationFunction.min_max_scaler_stats(
                feature_descriptive_stats, feature_name
            )
            transformation_function_instance.transformation_fn = partial(
                transformation_function_instance.transformation_fn,
                min_value=min_value,
                max_value=max_value,
            )
        elif transformation_function_instance.name == "standard_scaler":
            mean, std_dev = BuiltInTransformationFunction.standard_scaler_stats(
                feature_descriptive_stats, feature_name
            )
            transformation_function_instance.transformation_fn = partial(
                transformation_function_instance.transformation_fn,
                mean=mean,
                std_dev=std_dev,
            )
        elif transformation_function_instance.name == "robust_scaler":
            robust_scaler_stats = BuiltInTransformationFunction.robust_scaler_stats(
                feature_descriptive_stats, feature_name
            )
            transformation_function_instance.transformation_fn = partial(
                transformation_function_instance.transformation_fn,
                p25=robust_scaler_stats[24],
                p50=robust_scaler_stats[49],
                p75=robust_scaler_stats[74],
            )
        elif transformation_function_instance.name == "label_encoder":
            value_to_index = BuiltInTransformationFunction.encoder_stats(
                feature_descriptive_stats, feature_name
            )
            transformation_function_instance.transformation_fn = partial(
                transformation_function_instance.transformation_fn,
                value_to_index=value_to_index,
            )
        else:
            raise ValueError("Not implemented")

        return transformation_function_instance

    def populate_builtin_attached_fns(
        self, attached_transformation_fns, feature_descriptive_stats
    ):
        # TODO : Remove
        for ft_name in attached_transformation_fns:
            if self.is_builtin(attached_transformation_fns[ft_name]):
                # check if its built-in transformation function and populated with statistics arguments
                transformation_fn = self.populate_builtin_fn_arguments(
                    ft_name,
                    attached_transformation_fns[ft_name],
                    feature_descriptive_stats,
                )
                attached_transformation_fns[ft_name] = transformation_fn
        return attached_transformation_fns

    @staticmethod
    def infer_spark_type(output_type):
        # TODO : Move to hopsworks_udf
        if not output_type:
            return "STRING"  # STRING is default type for spark udfs

        if isinstance(output_type, str):
            if output_type.endswith("Type()"):
                return util.translate_legacy_spark_type(output_type)
            output_type = output_type.lower()

        if output_type in (str, "str", "string"):
            return "STRING"
        elif output_type in (bytes, "binary"):
            return "BINARY"
        elif output_type in (numpy.int8, "int8", "byte", "tinyint"):
            return "BYTE"
        elif output_type in (numpy.int16, "int16", "short", "smallint"):
            return "SHORT"
        elif output_type in (int, "int", "integer", numpy.int32):
            return "INT"
        elif output_type in (numpy.int64, "int64", "long", "bigint"):
            return "LONG"
        elif output_type in (float, "float"):
            return "FLOAT"
        elif output_type in (numpy.float64, "float64", "double"):
            return "DOUBLE"
        elif output_type in (
            datetime.datetime,
            numpy.datetime64,
            "datetime",
            "timestamp",
        ):
            return "TIMESTAMP"
        elif output_type in (datetime.date, "date"):
            return "DATE"
        elif output_type in (bool, "boolean", "bool"):
            return "BOOLEAN"
        else:
            raise TypeError("Not supported type %s." % output_type)

    @staticmethod
    def compute_transformation_fn_statistics(
        training_dataset_obj,
        builtin_tffn_features,
        label_encoder_features,
        feature_dataframe,
        feature_view_obj,
    ) -> statistics.Statistics:
        return training_dataset_obj._statistics_engine.compute_transformation_fn_statistics(
            td_metadata_instance=training_dataset_obj,
            columns=builtin_tffn_features,  # excluding label encoded features
            label_encoder_features=label_encoder_features,  # label encoded features only
            feature_dataframe=feature_dataframe,
            feature_view_obj=feature_view_obj,
        )

    @staticmethod
    def populate_builtin_transformation_functions(
        training_dataset, feature_view_obj, dataset
    ):
        return
        # TODO : Remove
        # check if there any transformation functions that require statistics attached to td features
        builtin_tffn_label_encoder_features = [
            ft_name
            for ft_name in training_dataset.transformation_functions
            if training_dataset._transformation_function_engine.is_builtin(
                training_dataset.transformation_functions[ft_name]
            )
            and training_dataset.transformation_functions[ft_name].name
            == "label_encoder"
        ]
        builtin_tffn_features = [
            ft_name
            for ft_name in training_dataset.transformation_functions
            if training_dataset._transformation_function_engine.is_builtin(
                training_dataset.transformation_functions[ft_name]
            )
            and training_dataset.transformation_functions[ft_name].name
            != "label_encoder"
        ]

        if builtin_tffn_features or builtin_tffn_label_encoder_features:
            if training_dataset.splits:
                # compute statistics before transformations are applied
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        builtin_tffn_features,
                        builtin_tffn_label_encoder_features,
                        dataset.get(training_dataset.train_split),
                        feature_view_obj,
                    )
                )
            else:
                # compute statistics before transformations are applied
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        builtin_tffn_features,
                        builtin_tffn_label_encoder_features,
                        dataset,
                        feature_view_obj,
                    )
                )
            # Populate builtin transformations (if any) with respective arguments
            return training_dataset._transformation_function_engine.populate_builtin_attached_fns(
                training_dataset.transformation_functions,
                stats.feature_descriptive_statistics,
            )

    def get_ready_to_use_transformation_fns(
        self,
        entity: Union[hsfs.feature_view.FeatureView, training_dataset.TrainingDataset],
        training_dataset_version: Optional[int] = None,
    ) -> Dict[
        str, hsfs.transformation_function_attached.TransformationFunctionAttached
    ]:
        is_feat_view = isinstance(entity, feature_view.FeatureView)
        if self._feature_view_api is None:
            self._feature_view_api = feature_view_api.FeatureViewApi(
                self._feature_store_id
            )
        if self._statistics_engine is None:
            self._statistics_engine = statistics_engine.StatisticsEngine(
                self._feature_store_id,
                entity_type="featureview" if is_feat_view else "trainingdataset",
            )
        # get attached transformation functions
        transformation_functions = (
            self.get_td_transformation_fn(entity)
            if isinstance(entity, training_dataset.TrainingDataset)
            else (self.get_fv_attached_transformation_fn(entity.name, entity.version))
        )
        is_stat_required = (
            len(
                set(self.BUILTIN_FN_NAMES).intersection(
                    set([tf.name for tf in transformation_functions.values()])
                )
            )
            > 0
        )
        if not is_stat_required:
            td_tffn_stats = None
        else:
            # if there are any built-in transformation functions get related statistics and
            # populate with relevant arguments
            # there should be only one statistics object with before_transformation=true
            if is_feat_view and training_dataset_version is None:
                raise ValueError(
                    "Training data version is required for transformation. Call `feature_view.init_serving(version)` "
                    "or `feature_view.init_batch_scoring(version)` to pass the training dataset version."
                    "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
                )
            td_tffn_stats = self._statistics_engine.get(
                entity,
                before_transformation=True,
                training_dataset_version=training_dataset_version,
            )

        if is_stat_required and td_tffn_stats is None:
            raise ValueError(
                "No statistics available for initializing transformation functions."
                + "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
            )

        transformation_fns = self.populate_builtin_attached_fns(
            transformation_functions,
            td_tffn_stats.feature_descriptive_statistics
            if td_tffn_stats is not None
            else None,
        )
        return transformation_fns

    def get_fv_attached_transformation_fn(
        self, fv_name: str, fv_version: int
    ) -> Dict[str, "transformation_function_attached.TransformationFunctionAttached"]:
        if self._feature_view_api is None:
            self._feature_view_api = feature_view_api.FeatureViewApi(
                self._feature_store_id
            )
            self._statistics_engine = statistics_engine.StatisticsEngine(
                self._feature_store_id,
                entity_type="featureview",
            )
        transformation_functions = (
            self._feature_view_api.get_attached_transformation_fn(fv_name, fv_version)
        )
        if isinstance(transformation_functions, list):
            transformation_functions_dict = dict(
                [
                    (tf.name, tf.transformation_function)
                    for tf in transformation_functions
                ]
            )
        else:
            transformation_functions_dict = {
                transformation_functions.name: transformation_functions.transformation_function
            }
        return transformation_functions_dict
