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

import numpy
import datetime
from functools import partial

from hsfs import training_dataset, training_dataset_feature
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import transformation_function_api, statistics_api
from hsfs.core.builtin_transformation_function import BuiltInTransformationFunction
from hsfs import util


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

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id
        self._transformation_function_api = (
            transformation_function_api.TransformationFunctionApi(feature_store_id)
        )
        self._statistics_api = statistics_api.StatisticsApi(
            feature_store_id, training_dataset.TrainingDataset.ENTITY_TYPE
        )

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
            transformation_fn_dict[
                attached_transformation_fn.name
            ] = attached_transformation_fn.transformation_function
        return transformation_fn_dict

    @staticmethod
    def attach_transformation_fn(training_dataset_obj=None, feature_view_obj=None):
        if training_dataset_obj:
            target_obj = training_dataset_obj  # todo why provide td and fv just to convert to target_obj?
        else:
            target_obj = feature_view_obj
        # If provided feature matches column with prefix, then attach transformation function.
        # If provided feature matches only one column without prefix, then attach transformation function. (For
        # backward compatibility purpose, as of v3.0, features are matched to columns without prefix.)
        # If provided feature matches multiple columns without prefix, then raise exception because it is ambiguous.
        prefix_feature_map = {}
        feature_map = {}
        for feat in target_obj.query.features:
            prefix_feature_map[feat.name] = (
                feat.name,
                target_obj.query._left_feature_group,
            )
        for join in target_obj.query.joins:
            for feat in join.query.features:
                if join.prefix:
                    prefix_feature_map[join.prefix + feat.name] = (
                        feat.name,
                        join.query._left_feature_group,
                    )
                feature_map[feat.name] = feature_map.get(feat.name, []) + [
                    join.query._left_feature_group
                ]

        if target_obj._transformation_functions:
            for (
                feature_name,
                transformation_fn,
            ) in target_obj._transformation_functions.items():
                if feature_name in target_obj.labels:  # todo td does not have labels
                    raise ValueError(
                        "Online transformations for training dataset labels are not supported."
                    )
                if feature_name in prefix_feature_map:
                    target_obj._features.append(
                        training_dataset_feature.TrainingDatasetFeature(
                            name=feature_name,
                            feature_group_feature_name=prefix_feature_map[feature_name][
                                0
                            ],
                            featuregroup=prefix_feature_map[feature_name][1],
                            type=transformation_fn.output_type,
                            label=False,
                            transformation_function=transformation_fn,
                        )
                    )
                elif feature_name in feature_map:
                    if len(feature_map[feature_name]) > 1:
                        raise FeatureStoreException(
                            TransformationFunctionEngine.AMBIGUOUS_FEATURE_ERROR.format(
                                feature_name
                            )
                        )
                    target_obj._features.append(
                        training_dataset_feature.TrainingDatasetFeature(
                            name=feature_name,
                            feature_group_feature_name=feature_name,
                            featuregroup=feature_map[feature_name][0],
                            type=transformation_fn.output_type,
                            label=False,
                            transformation_function=transformation_fn,
                        )
                    )
                else:
                    raise FeatureStoreException(
                        TransformationFunctionEngine.FEATURE_NOT_EXIST_ERROR.format(
                            feature_name
                        )
                    )

    def is_builtin(self, transformation_fn_instance):
        return (
            transformation_fn_instance.name in self.BUILTIN_FN_NAMES
            and transformation_fn_instance.version == 1
        )

    @staticmethod
    def populate_builtin_fn_arguments(
        feature_name, transformation_function_instance, stat_content
    ):
        if transformation_function_instance.name == "min_max_scaler":
            min_value, max_value = BuiltInTransformationFunction.min_max_scaler_stats(
                stat_content, feature_name
            )
            transformation_function_instance.transformation_fn = partial(
                transformation_function_instance.transformation_fn,
                min_value=min_value,
                max_value=max_value,
            )
        elif transformation_function_instance.name == "standard_scaler":
            mean, std_dev = BuiltInTransformationFunction.standard_scaler_stats(
                stat_content, feature_name
            )
            transformation_function_instance.transformation_fn = partial(
                transformation_function_instance.transformation_fn,
                mean=mean,
                std_dev=std_dev,
            )
        elif transformation_function_instance.name == "robust_scaler":
            robust_scaler_stats = BuiltInTransformationFunction.robust_scaler_stats(
                stat_content, feature_name
            )
            transformation_function_instance.transformation_fn = partial(
                transformation_function_instance.transformation_fn,
                p25=robust_scaler_stats[24],
                p50=robust_scaler_stats[49],
                p75=robust_scaler_stats[74],
            )
        elif transformation_function_instance.name == "label_encoder":
            value_to_index = BuiltInTransformationFunction.encoder_stats(
                stat_content, feature_name
            )
            transformation_function_instance.transformation_fn = partial(
                transformation_function_instance.transformation_fn,
                value_to_index=value_to_index,
            )
        else:
            raise ValueError("Not implemented")

        return transformation_function_instance

    def populate_builtin_attached_fns(self, attached_transformation_fns, stat_content):
        for ft_name in attached_transformation_fns:
            if self.is_builtin(attached_transformation_fns[ft_name]):
                # check if its built-in transformation function and populated with statistics arguments
                transformation_fn = self.populate_builtin_fn_arguments(
                    ft_name, attached_transformation_fns[ft_name], stat_content
                )
                attached_transformation_fns[ft_name] = transformation_fn
        return attached_transformation_fns

    @staticmethod
    def infer_spark_type(output_type):
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
    ):
        return training_dataset_obj._statistics_engine.compute_transformation_fn_statistics(
            td_metadata_instance=training_dataset_obj,
            columns=builtin_tffn_features,
            label_encoder_features=label_encoder_features,
            feature_dataframe=feature_dataframe,
            feature_view_obj=feature_view_obj,
        )

    @staticmethod
    def populate_builtin_transformation_functions(
        training_dataset, feature_view_obj, dataset
    ):
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
                training_dataset.transformation_functions, stats.content
            )
