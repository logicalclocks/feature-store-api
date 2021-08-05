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
                training_dataset._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature_name,
                        label=False,
                        transformation_function=transformation_fn,
                    )
                )

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
