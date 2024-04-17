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
import inspect
import warnings
from typing import Callable, List, Union

from hsfs import engine
from hsfs.client.exceptions import FeatureStoreException


def hopsworks_udf(return_type: Union[List[type], type]):
    def wrapper(func: Callable):
        udf = HopsworksUdf(func=func, return_type=return_type)
        return udf

    return wrapper


class HopsworksUdf:
    """
    Metadata class to store information about UDF
    """

    PYTHON_SPARK_TYPE_MAPPING = {
        str: "string",
        int: "int",
        float: "float",
        # "timestamp": TimestampType(),
        bool: "boolean",
        # "date": DateType(),
        # "binary": BinaryType(),
    }

    def __init__(
        self, func: Callable, return_type: Union[List[type], type], name: str = None
    ):
        self.udf_function: Callable = func
        if name is None:
            self.function_name: str = func.__name__
        else:
            self.function_name: str = name
        self.return_type: Union[List[type], type] = return_type
        self.function_source: str = self._remove_argument(
            HopsworksUdf._extract_source_code(self.udf_function), "statistics"
        )
        # TODO : Add a getter functions
        self.transformation_features: List[str] = (
            HopsworksUdf._extract_function_arguments(self.function_source)
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
        if not callable(udf_function):
            # TODO : Think about a better text for the raised error
            raise ValueError("transformation function must be callable")

        try:
            module_imports = HopsworksUdf._get_module_imports(
                HopsworksUdf._get_module_path(udf_function.__module__)
            )
        except Exception:
            module_imports = ""
            # TODO : Check if warning is actually required.
            warnings.warn(
                "Passed UDF defined in a Jupyter notebook. Cannot extract dependices from a notebook. Please make sure to import all dependcies for the UDF inside the code.",
                stacklevel=2,
            )

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
        arg_list = [arg.strip() for arg in arg_list]
        return arg_list

    def _remove_argument(self, source_code: str, arg_to_remove: str):
        """ "
        Function to remove statistics arguments from passed udf and type hinting.
        Statistics arguments are removed since pandas UDF's do not accept extra arguments.
        Statistics parameters are dynamically injected into the function scope.
        """

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
        arg_list = [
            arg.split(":")[0].strip()
            for arg in arg_list
            if (
                arg_to_remove not in list(map(str.strip, arg.split(" ")))
                and arg_to_remove not in list(map(str.strip, arg.split(":")))
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

        # Modify the source code to reflect the changes
        source_code[signature_line] = new_signature

        # Removing test before function signatre since they are decorators
        source_code = source_code[signature_line:]

        # Reconstruct the modified function as a string
        modified_source = "\n".join(source_code)

        # Define a new function with the modified source code
        return modified_source

    @staticmethod
    def get_spark_type(python_type: type):
        return HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING[python_type]

    def create_pandas_udf_return_schema_from_list(self, return_types: List[type]):
        return ", ".join(
            [
                f'`{self.function_name}<{"-".join(self.transformation_features)}>{i}` {HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING[return_types[i]]}'
                for i in range(len(return_types))
            ]
        )

    def hopsworksUdf_wrapper(self, **statistics):
        # TODO : clean this up
        if isinstance(self.return_type, List):
            self.function_source = "\t".join(self.function_source.splitlines(True))
            self.code = f"""def renaming_wrapper(*args):
    import pandas as pd
    {self.function_source}
    df = {self.function_name}(*args)
    df = df.rename(columns = {{f'{{df.columns[i]}}':f'{self.function_name}<{"-".join(self.transformation_features)}>{{i}}' for i in range(len(df.columns))}})
    return df"""
        else:
            self.code = self.function_source
        scope = __import__("__main__").__dict__
        scope.update(**statistics)
        exec(self.code, scope)
        if isinstance(self.transformation_features, List):
            return eval("renaming_wrapper", scope)
        else:
            return eval(self.function_name, scope)

    def __call__(self, *args: List[str]):
        # TODO : Raise an execption if the number of features are incorrect.
        for arg in args:
            if not isinstance(arg, str):
                raise FeatureStoreException(
                    f'Feature names provided must be string "{arg}" is not string'
                )

        self.transformation_features = list(args)
        return self

    def get_udf(self, statistics):
        if engine.get_type() in ["hive", "python", "training"]:
            return self.hopsworksUdf_wrapper(statistics=statistics)
        else:
            from pyspark.sql.functions import pandas_udf

            # TODO : Make this proper
            return pandas_udf(
                f=self.hopsworksUdf_wrapper(statistics=statistics),
                returnType=self.create_pandas_udf_return_schema_from_list(
                    self.return_type
                ),
            )
