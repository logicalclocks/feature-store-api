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

from hsfs import training_dataset, training_dataset_feature, transformation_function
from hsfs.core import transformation_function_api, statistics_api
from hsfs.client.exceptions import RestAPIError
from hsfs.core.builtin_transformation_function import BuiltInTransformationFunction


class TransformationFunctionEngine:
    BUILTIN_FN_NAMES = [
        "min_max_scaler",
        "standard_scaler",
        "robust_scaler",
        "label_encoder",
    ]

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
        for transformation_fn_instance in transformation_fn_instances:
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

    def attach_transformation_fn(self, training_dataset):
        if training_dataset._transformation_functions:
            for (
                feature_name,
                transformation_fn,
            ) in training_dataset._transformation_functions.items():
                if feature_name in training_dataset.label:
                    raise ValueError(
                        "Online transformations for training dataset labels are not supported."
                    )
                training_dataset._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature_name,
                        feature_group_feature_name=feature_name,
                        type=transformation_fn.output_type,
                        label=False,
                        transformation_function=transformation_fn,
                    )
                )

    def register_builtin_transformation_fns(self):
        for name in self.BUILTIN_FN_NAMES:
            try:
                self._transformation_function_api.get_transformation_fn(name, 1)[0]
            except RestAPIError as e:
                if (
                    e.response.json().get("errorMsg")
                    == "Transformation function does not exist"
                ):
                    builtin_fn = BuiltInTransformationFunction(name)
                    (
                        builtin_source_code,
                        output_type,
                    ) = builtin_fn.generate_source_code()
                    transformation_fn_instance = (
                        transformation_function.TransformationFunction(
                            featurestore_id=self._feature_store_id,
                            name=name,
                            version=1,
                            output_type=output_type,
                            builtin_source_code=builtin_source_code,
                        )
                    )
                    self._transformation_function_api.register_transformation_fn(
                        transformation_fn_instance
                    )
                elif (
                    e.response.json().get("errorMsg")
                    == "The provided transformation function name and version already exists"
                ):
                    Warning(e.response.json().get("errorMsg"))

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
        if output_type in (str, "str", "string"):
            return "StringType()"
        elif output_type in (bytes,):
            return "BinaryType()"
        elif output_type in (numpy.int8, "int8", "byte"):
            return "ByteType()"
        elif output_type in (numpy.int16, "int16", "short"):
            return "ShortType()"
        elif output_type in (int, "int", numpy.int, numpy.int32):
            return "IntegerType()"
        elif output_type in (numpy.int64, "int64", "long", "bigint"):
            return "LongType()"
        elif output_type in (float, "float", numpy.float):
            return "FloatType()"
        elif output_type in (numpy.float64, "float64", "double"):
            return "DoubleType()"
        elif output_type in (datetime.datetime, numpy.datetime64):
            return "TimestampType()"
        elif output_type in (datetime.date,):
            return "DateType()"
        elif output_type in (bool, "boolean", "bool", numpy.bool):
            return "BooleanType()"
        else:
            raise TypeError("Not supported type %s." % output_type)
