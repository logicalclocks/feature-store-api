#
#   Copyright 2024 Hopsworks AB
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

import ast
import copy
import inspect
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

import humps
from hsfs import engine, util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


def hopsworks_udf(return_type: Union[List[type], type]):
    def wrapper(func: Callable):
        udf = HopsworksUdf(func=func, return_type=return_type)
        return udf

    return wrapper


@dataclass
class TransformationFeature:
    feature_name: str
    statistic_argument_name: Optional[str]

    def to_dict(self):
        return {
            "feature_name": self.feature_name,
            "statistic_argument_name": self.statistic_argument_name,
        }


class HopsworksUdf:
    """
    Metadata class to store information about UDF
    """

    # TODO : Complete this
    PYTHON_SPARK_TYPE_MAPPING = {
        str: "string",
        int: "int",
        float: "double",
        # "timestamp": TimestampType(),
        bool: "boolean",
        # "date": DateType(),
        # "binary": BinaryType(),
    }

    STRING_PYTHON_TYPES_MAPPING = {"str": str, "int": int, "float": float, "bool": bool}

    def __init__(
        self,
        func: Union[Callable, str],
        return_type: Union[List[type], type],
        name: str = None,
        transformation_features: List[TransformationFeature] = None,
    ):
        if name is None:
            self._function_name: str = func.__name__
        else:
            self._function_name: str = name

        self._statistics: Optional[Dict[str, FeatureDescriptiveStatistics]] = dict()

        self._return_type: Union[List[type], type] = return_type

        if isinstance(func, Callable):
            self._function_source: str = HopsworksUdf._extract_source_code(func)
        else:
            self._function_source: str = func

        if transformation_features:
            self._transformation_features: List[TransformationFeature] = (
                transformation_features
            )
        else:
            self._transformation_features: List[TransformationFeature] = (
                HopsworksUdf._extract_function_arguments(self.function_source)
            )

        self._function_source = self._remove_argument(
            self.function_source, "statistics"
        )
        HopsworksUdf.validate_arguments(self.return_type)

    def get_transformation_features(self):
        return self.transformation_features

    @staticmethod
    def validate_arguments(return_type):
        if isinstance(return_type, list):
            for python_type in return_type:
                if not isinstance(python_type, type):
                    raise FeatureStoreException(
                        f'Return types provided must be a python type or a list of python types. "{python_type}" is not python type'
                    )
        else:
            if not isinstance(return_type, type):
                raise FeatureStoreException(
                    f'Return types provided must be a python type or a list of python types. "{return_type}" is not python type or a list'
                )

    @staticmethod
    def _get_module_imports(path):
        imports = []
        with open(path) as fh:
            root = ast.parse(fh.read(), path)

        for node in ast.iter_child_nodes(root):
            if isinstance(node, ast.Import):
                imported_module = False
            elif isinstance(node, ast.ImportFrom):
                imported_module = node.module
            else:
                continue

            for n in node.names:
                if imported_module:
                    import_line = "from " + imported_module + " import " + n.name
                elif n.asname:
                    import_line = "import " + n.name + " as " + n.asname
                else:
                    import_line = "import " + n.name
                imports.append(import_line)
        return imports

    @staticmethod
    def _get_module_path(module_name):
        def _get_module_path(module):
            return module.__file__

        module_path = {}
        exec(
            f'import {module_name}\nmodule_path["path"] = _get_module_path({module_name})'
        )
        return module_path["path"]

    @staticmethod
    def _extract_source_code(udf_function):
        try:
            module_imports = HopsworksUdf._get_module_imports(
                HopsworksUdf._get_module_path(udf_function.__module__)
            )
        except Exception:
            module_imports = ""
            # TODO : Check if warning is actually required.
            # warnings.warn(
            #    "Passed UDF defined in a Jupyter notebook. Cannot extract dependices from a notebook. Please make sure to import all dependcies for the UDF inside the code.",
            #    stacklevel=2,
            # )

        function_code = inspect.getsource(udf_function)
        source_code = "\n".join(module_imports) + "\n" + function_code

        return source_code

    @staticmethod
    def _extract_function_arguments(source_code):
        # Get source code of the original function
        source_code = source_code.split("\n")

        # Find the line where the function signature is defined
        for i, line in enumerate(source_code):
            if line.strip().startswith("def "):
                signature_line = i
                break

        # Parse the function signature to remove the specified argument
        signature = source_code[signature_line]
        arg_list = signature.split("(")[1].split(")")[0].split(",")

        arg_list = [arg.split(":")[0].strip() for arg in arg_list]

        return [
            TransformationFeature(
                arg, f"statistics_{arg}" if f"statistics_{arg}" in arg_list else None
            )
            for arg in arg_list
            if not arg.startswith("statistics")
        ]

    def _remove_argument(self, source_code: str, arg_to_remove: str):
        """ "
        Function to remove statistics arguments from passed udf and type hinting.
        Statistics arguments are removed since pandas UDF's do not accept extra arguments.
        Statistics parameters are dynamically injected into the function scope.
        """

        # Get source code of the original function
        source_code = source_code.split("\n")

        signature_start_line = None
        signature_end_line = None
        # Find the line where the function signature is defined
        for i, line in enumerate(source_code):
            if line.strip().startswith("def "):
                signature_start_line = i
            if signature_start_line is not None and ")" in line:
                signature_end_line = i
                break

        # Parse the function signature to remove the specified argument
        signature = "".join(source_code[signature_start_line : signature_end_line + 1])
        arg_list = signature.split("(")[1].split(")")[0].split(",")
        arg_list = [
            arg.split(":")[0].strip()
            for arg in arg_list
            if (
                arg_to_remove not in list(map(str.strip, arg.split(" ")))
                and arg_to_remove not in list(map(str.strip, arg.split(":")))
                and arg_to_remove not in list(map(str.strip, arg.split("_")))
                and arg.strip() != arg_to_remove
            )
        ]

        # Reconstruct the function signature
        new_signature = (
            signature.split("(")[0]
            + "("
            + ", ".join(arg_list)
            + ")"
            + signature.split(")")[1]
        )
        # Reconstruct the modified function as a string
        modified_source = (
            new_signature + "\n" + "\n".join(source_code[signature_end_line + 1 :])
        )

        # Define a new function with the modified source code
        return modified_source

    @staticmethod
    def get_spark_type(python_type: type):
        return HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING[python_type]

    def create_pandas_udf_return_schema_from_list(self, return_types: List[type]):
        if isinstance(return_types, List):
            return ", ".join(
                [
                    f'`{self.function_name}<{"-".join(self.transformation_features)}>{i}` {HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING[return_types[i]]}'
                    for i in range(len(return_types))
                ]
            )
        else:
            return f"{HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING[return_types]}"

    def hopsworksUdf_wrapper(self):
        # TODO : clean this up
        function_source = "\t".join(self.function_source.splitlines(True))
        if isinstance(self.return_type, List):
            code = f"""def renaming_wrapper(*args):
    import pandas as pd
    {function_source}
    df = {self.function_name}(*args)
    df = df.rename(columns = {{f'{{df.columns[i]}}':f'{self.function_name}<{"-".join(self.transformation_features)}>{{i}}' for i in range(len(df.columns))}})
    return df"""
        else:
            code = f"""def renaming_wrapper(*args):
    import pandas as pd
    {function_source}
    df = {self.function_name}(*args)
    df = df.rename(f'{self.function_name}<{"-".join(self.transformation_features)}>')
    return df"""
        scope = __import__("__main__").__dict__
        scope.update(self.transformation_statistics)
        exec(code, scope)
        return eval("renaming_wrapper", scope)

    def __call__(self, *args: List[str]):
        # TODO : Raise an execption if the number of features are incorrect.
        if len(args) != len(self.transformation_features):
            raise FeatureStoreException(
                "Number of features provided does not match the number of features provided in the UDF definition"
            )
        for arg in args:
            if not isinstance(arg, str):
                raise FeatureStoreException(
                    f'Feature names provided must be string "{arg}" is not string'
                )
        udf = copy.deepcopy(
            self
        )  # TODO : Clean this copy is needed so that if the uses the same function to multiple feature, if copy not done then all variable would share the same traanformation feature,
        udf._transformation_features = [
            TransformationFeature(
                new_feature_name, transformation_feature.statistic_argument_name
            )
            for transformation_feature, new_feature_name in zip(
                self._transformation_features, args
            )
        ]
        return udf

    def get_udf(self):
        if engine.get_type() in ["hive", "python", "training"]:
            return self.hopsworksUdf_wrapper()
        else:
            from pyspark.sql.functions import pandas_udf

            # TODO : Make this proper
            return pandas_udf(
                f=self.hopsworksUdf_wrapper(),
                returnType=self.create_pandas_udf_return_schema_from_list(
                    self.return_type
                ),
            )

    def to_dict(self):
        return {
            "sourceCode": self.function_source,
            "outputTypes": [python_type.__name__ for python_type in self.return_type]
            if isinstance(self.return_type, List)
            else self.return_type.__name__,
            "transformationFeatures": self.transformation_features,
            "name": self._function_name,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @classmethod
    def from_response_json(
        cls: "HopsworksUdf", json_dict: Dict[str, Any]
    ) -> "HopsworksUdf":
        json_decamelized = humps.decamelize(json_dict)
        function_source_code = json_decamelized["source_code"]
        function_name = json_decamelized["name"]
        return_type = json_decamelized["output_types"]
        transformation_features = json_decamelized["transformation_features"].split(",")

        hopsworks_udf = cls(
            func=function_source_code,
            return_type=[
                cls.STRING_PYTHON_TYPES_MAPPING[python_type]
                for python_type in return_type
            ]
            if isinstance(return_type, List)
            else cls.STRING_PYTHON_TYPES_MAPPING[return_type],
            name=function_name,
        )

        return hopsworks_udf(*transformation_features)

    @property
    def return_type(self):
        return self._return_type

    @property
    def function_name(self):
        return self._function_name

    @property
    def function_source(self):
        return self._function_source

    @property
    def statistics_required(self):
        return bool(self.statistics_features)

    @property
    def transformation_statistics(self):
        return self._statistics

    @property
    def transformation_features(self):
        return [
            transformation_feature.feature_name
            for transformation_feature in self._transformation_features
        ]

    @property
    def statistics_features(self):
        return [
            transformation_feature.feature_name
            for transformation_feature in self._transformation_features
            if transformation_feature.statistic_argument_name is not None
        ]

    @property
    def statistics_argument_mapping(self):
        return {
            transformation_feature.feature_name: transformation_feature.statistic_argument_name
            for transformation_feature in self._transformation_features
        }

    @transformation_statistics.setter
    def transformation_statistics(self, statistics: List[FeatureDescriptiveStatistics]):
        # TODO : Clean this up
        self._statistics = dict()
        for stat in statistics:
            if stat.feature_name in self.statistics_argument_mapping.keys():
                self._statistics[
                    self.statistics_argument_mapping[stat.feature_name]
                ] = stat
