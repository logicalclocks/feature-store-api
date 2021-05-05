#
#   Copyright 2020 Logical Clocks AB
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

from hsfs import training_dataset_feature
from hsfs.core import transformation_function_api


class TransformationFunctionEngine:
    def __init__(self, feature_store_id):
        self._transformation_function_api = (
            transformation_function_api.TransformationFunctionApi(feature_store_id)
        )

    def save(self, transformation_fn_instance):
        if not callable(transformation_fn_instance.transformation_fn):
            raise ValueError("transformer must be callable")
        self._transformation_function_api.register_transformation_fn(
            transformation_fn_instance
        )

    def get_transformation_fn(self, name, version=None):
        transformation_fn_instances = (
            self._transformation_function_api.get_transformation_fn(name, version)
        )
        if version is not None:
            return transformation_fn_instances[0]
        else:
            transformation_fns = []
            for transformation_fn_instance in transformation_fn_instances:
                transformation_fn_dict = {
                    "name": transformation_fn_instance.name,
                    "version": transformation_fn_instance.version,
                    "output_type": transformation_fn_instance.output_type,
                    "transformation_function": transformation_fn_instance,
                }
                transformation_fns.append(transformation_fn_dict)
            return transformation_fns

    def get_transformation_fns(self):
        transformation_fn_instances = (
            self._transformation_function_api.get_transformation_fn(
                name=None, version=None
            )
        )
        transformation_fns = []
        for transformation_fn_instance in transformation_fn_instances:
            transformation_fn_dict = {
                "name": transformation_fn_instance.name,
                "version": transformation_fn_instance.version,
                "output_type": transformation_fn_instance.output_type,
                "transformation_function": transformation_fn_instance,
            }
            transformation_fns.append(transformation_fn_dict)
        return transformation_fns

    def delete(self, transformation_function_instance):
        self._transformation_function_api.delete(transformation_function_instance)

    @staticmethod
    def get_training_dataset_transformation_fn(training_dataset):
        training_dataset = (
            transformation_function_api.get_training_dataset_transformation_fn(
                training_dataset
            )
        )
        transformation_fn_dict = {}
        for feature in training_dataset._features:
            if feature.transformation_function is not None:
                transformation_fn_dict[feature.name] = feature.transformation_function
        return transformation_fn_dict

    def attach_transformation_fn(self, training_dataset):
        if training_dataset._transformation_functions:
            for (
                feature_name,
                transformation_fn,
            ) in training_dataset._transformation_functions.items():
                self._validate_feature_exists(training_dataset._querydto, feature_name)
                training_dataset._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature_name,
                        label=False,
                        transformation_function=transformation_fn,
                    )
                )

    @staticmethod
    def _validate_feature_exists(td_query_instance, feature_name):
        transfrom_feature = None
        # Fist inspect leftmost feature group
        for td_feature in td_query_instance._left_feature_group.features:
            if feature_name == td_feature.name:
                transfrom_feature = td_feature
                break

        # If not found iterate over joins
        if transfrom_feature is None:
            for join in td_query_instance._joins:
                for td_feature in join.query._left_feature_group.features:
                    if feature_name == td_feature.name:
                        transfrom_feature = td_feature
                        break

        if transfrom_feature is None:
            raise Exception(
                "Provided feature %s doesn't exist in training dataset query"
                % feature_name
            )

    @staticmethod
    def infer_spark_type(ptype):
        if ptype in (str, "str", "string"):
            return "StringType()"
        elif ptype in (bytes,):
            return "BinaryType()"
        elif ptype in (numpy.int8, "int8", "byte"):
            return "ByteType()"
        elif ptype in (numpy.int16, "int16", "short"):
            return "ShortType()"
        elif ptype in (int, "int", numpy.int, numpy.int32):
            return "IntegerType()"
        elif ptype in (numpy.int64, "int64", "long", "bigint"):
            return "LongType()"
        elif ptype in (float, "float", numpy.float):
            return "FloatType()"
        elif ptype in (numpy.float64, "float64", "double"):
            return "DoubleType()"
        elif ptype in (datetime.datetime, numpy.datetime64):
            return "TimestampType()"
        elif ptype in (datetime.date,):
            return "DateType()"
        elif ptype in (bool, "boolean", "bool", numpy.bool):
            return "BooleanType()"
        else:
            raise TypeError("Not supported type %s." % ptype)
