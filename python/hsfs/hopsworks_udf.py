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
import warnings
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import humps
from hsfs import engine, util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.decorators import typechecked


def hopsworks_udf(output_type: Union[List[type], type]) -> "HopsworksUdf":
    """
    Create an User Defined Function that can be and used within the Hopsworks Feature Store.

    Hopsworks UDF's are user defined functions that executes as 'pandas_udf' when executing
    in spark engine and as pandas functions in the python engine. A Hopsworks udf is defined
    using the `hopsworks_udf` decorator. The outputs of the defined UDF must be mentioned in the
    decorator as a list of python types.


    !!! example
        ```python
        from hsfs.hopsworks_udf import hopsworks_udf

       @hopsworks_udf(float)
        def add_one(data1 : pd.Series):
            return data1 + 1
        ```

    # Arguments
        output_type: `list`. The output types of the defined UDF

    # Returns
        `HopsworksUdf`: The metadata object for hopsworks UDF's.

    # Raises
        `hsfs.client.exceptions.FeatureStoreException` : If unable to create UDF.
    """

    def wrapper(func: Callable) -> HopsworksUdf:
        udf = HopsworksUdf(func=func, output_types=output_type)
        return udf

    return wrapper


@dataclass
class TransformationFeature:
    """
    Mapping of feature names to their corresponding statistics argument names in the code.

    The statistic_argument_name for a feature name would be None if the feature does not need statistics.

    Attributes
    ----------
        feature_name (str) : Name of the feature.
        statistic_argument_name (str) : Name of the statistics argument in the code for the feature specified in the feature name.
    """

    feature_name: str
    statistic_argument_name: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feature_name": self.feature_name,
            "statistic_argument_name": self.statistic_argument_name,
        }


@typechecked
class HopsworksUdf:
    """
    Meta data for user defined functions.

    Stores meta data required to execute the user defined function in both spark and python engine.
    The class generates uses the metadata to dynamically generate user defined functions based on the
    engine it is executed in.

    Attributes
    ----------
        output_type (List[str]) : Output types of the columns returned from the UDF.
        function_name (str) : Name of the UDF
        statistics_required (bool) : True if statistics is required for any of the parameters of the UDF.
        transformation_statistics (Dict[str, FeatureDescriptiveStatistics]): Dictionary that maps the statistics_argument name in the function to the actual statistics variable.
        transformation_features (List[str]) : List of feature names to which the transformation function would be applied.
        statistics_features (List[str]) : List of feature names that requires statistics.
    """

    # Mapping for converting python types to spark types - required for creating pandas UDF's.
    PYTHON_SPARK_TYPE_MAPPING = {
        str: "string",
        int: "bigint",
        float: "double",
        bool: "boolean",
        datetime: "timestamp",
        time: "timestamp",
        date: "date",
    }

    def __init__(
        self,
        func: Union[Callable, str],
        output_types: Union[List[type], type, List[str], str],
        name: Optional[str] = None,
        transformation_features: Optional[List[TransformationFeature]] = None,
    ):
        self._output_types: List[str] = HopsworksUdf._validate_and_convert_output_types(
            output_types
        )

        self._function_name: str = func.__name__ if name is None else name

        self._function_source: str = (
            HopsworksUdf._extract_source_code(func)
            if isinstance(func, Callable)
            else func
        )

        self._transformation_features: List[TransformationFeature] = (
            HopsworksUdf._extract_function_arguments(self._function_source)
            if not transformation_features
            else transformation_features
        )

        self._formatted_function_source = HopsworksUdf._format_source_code(
            self._function_source, self._transformation_features
        )

        self._output_column_names: List[str] = self._get_output_column_names()

        self._statistics: Optional[Dict[str, FeatureDescriptiveStatistics]] = None

    @staticmethod
    def _validate_and_convert_output_types(
        output_types: Union[List[type], List[str]],
    ) -> List[str]:
        """
        Function that takes in a type or list of types validates if it is supported and return a list of strings

        # Arguments
            output_types: `list`. List of python types.

        # Raises
            `hsfs.client.exceptions.FeatureStoreException` : If the any of the output type is invalid
        """
        convert_output_types = []
        output_types = (
            output_types if isinstance(output_types, List) else [output_types]
        )
        for output_type in output_types:
            if (
                output_type not in HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING.keys()
                and output_type not in HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING.values()
            ):
                raise FeatureStoreException(
                    f"Output type {output_type} is not supported. Please refer to DOCUMENTATION to get more information on the supported types."
                )
            convert_output_types.append(
                output_type
                if isinstance(output_type, str)
                else HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING[output_type]
            )
        return convert_output_types

    @staticmethod
    def _get_module_imports(path: str) -> List[str]:
        """Function that extracts the imports used in the python file specified in the path.

        # Arguments
            path: `str`. Path to python file from which imports are to be extracted.

        # Returns
            `List[str]`: A list of string that contains the import statement using in the file.
        """
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
    def _get_module_path(module_name: str) -> str:
        """
        Function that returns the path to the source code of a python module.

        Cannot extract path if the module is defined in a jupyter notebook since it is currently impossible find the path of a jupyter notebook.(https://github.com/ipython/ipython/issues/10123)

        # Arguments
            path: `str`. Path to python file from which imports are to be extracted.
        # Raises
            AttributeError : If the provided module is defined in a jupyter notebook.
        # Returns
            `str`: a string that contains the path to the module
        """

        def _get_module_path(module):
            return module.__file__

        module_path = {}
        exec(
            f'import {module_name}\nmodule_path["path"] = _get_module_path({module_name})'
        )
        return module_path["path"]

    @staticmethod
    def _extract_source_code(udf_function: Callable) -> str:
        """
        Function to extract the source code of the function along with the imports used in the file.

        The module imports cannot be extracted if the function is defined in a jupyter notebook.

        # Arguments
            udf_function: `Callable`. Function for which the source code must be extracted.
        # Returns
            `str`: a string that contains the source code of function along with the extracted module imports.
        """
        try:
            module_imports = HopsworksUdf._get_module_imports(
                HopsworksUdf._get_module_path(udf_function.__module__)
            )
        except AttributeError:
            module_imports = [""]
            warnings.warn(
                "Passed UDF defined in a Jupyter notebook. Cannot extract import dependencies from a notebook. Please make sure to import all dependencies for the UDF inside the function.",
                stacklevel=2,
            )

        function_code = inspect.getsource(udf_function)
        source_code = "\n".join(module_imports) + "\n" + function_code

        return source_code

    @staticmethod
    def _parse_function_signature(source_code: str) -> Tuple[List[str], str, int, int]:
        """
        Function to parse the source code to extract the argument along with the start and end line of the function signature

        # Arguments
            source_code: `str`. Source code of a function.
        # Returns
            `List[str]`: List of function arguments
            `str`: function signature
            `int`: starting line number of function signature
            `int`: ending line number of function signature

        """
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
        signature = "".join(
            [
                code.split("#")[0]
                for code in source_code[signature_start_line : signature_end_line + 1]
            ]
        )
        arg_list = signature.split("(")[1].split(")")[0].split(",")
        return arg_list, signature, signature_start_line, signature_end_line

    @staticmethod
    def _extract_function_arguments(source_code: str) -> List[TransformationFeature]:
        """
        Function to extract the argument names from a provided function source code.

        # Arguments
            source_code: `str`. Source code of a function.
        # Returns
            `List[TransformationFeature]`: List of TransformationFeature that provide a mapping from feature names to corresponding statistics parameters if any is present.
        """
        # Get source code of the original function
        arg_list, _, _, _ = HopsworksUdf._parse_function_signature(source_code)

        if arg_list == [""]:
            raise FeatureStoreException(
                "No arguments present in the provided user defined function. Please provide at least one argument in the defined user defined function."
            )

        arg_list = [arg.split(":")[0].strip() for arg in arg_list]

        for arg in arg_list:
            if arg.startswith("statistics"):
                if arg.split("statistics_")[1] not in arg_list:
                    raise FeatureStoreException(
                        f"No argument corresponding to statistics parameter '{arg}' present in function definition."
                    )

        return [
            TransformationFeature(
                arg, f"statistics_{arg}" if f"statistics_{arg}" in arg_list else None
            )
            for arg in arg_list
            if not arg.startswith("statistics")
        ]

    @staticmethod
    def _format_source_code(
        source_code: str, transformation_features: List[TransformationFeature]
    ) -> str:
        """
        Function that parses the existing source code to remove statistics parameter and remove all decorators and type hints from the function source code.

        # Arguments
            source_code: `str`. Source code of a function.
            transformation_features `List[TransformationFeature]`: List of transformation features provided in the function argument.
        # Returns
            `str`: Source code that does not contain any decorators, type hints or statistics parameters.
        """

        _, signature, _, signature_end_line = HopsworksUdf._parse_function_signature(
            source_code
        )

        arg_list = [feature.feature_name for feature in transformation_features]

        # Reconstruct the function signature
        new_signature = (
            signature.split("(")[0].strip() + "(" + ", ".join(arg_list) + "):"
        )
        source_code = source_code.split("\n")
        # Reconstruct the modified function as a string
        modified_source = (
            new_signature + "\n" + "\n\t".join(source_code[signature_end_line + 1 :])
        )

        # Define a new function with the modified source code
        return modified_source

    def _get_output_column_names(self) -> str:
        """
        Function that generates feature names for the transformed features

        # Returns
            `List[str]`: List of feature names for the transformed columns
        """
        if len(self.output_types) > 1:
            return [
                f'{self.function_name}_{"-".join(self.transformation_features)}_{i}'
                for i in range(len(self.output_types))
            ]
        else:
            return [f'{self.function_name}_{"-".join(self.transformation_features)}_']

    def _create_pandas_udf_return_schema_from_list(self) -> str:
        """
        Function that creates the return schema required for executing the defined UDF's as pandas UDF's in Spark.

        # Returns
            `str`: DDL-formatted type string that denotes the return types of the user defined function.
        """
        if len(self.output_types) > 1:
            return ", ".join(
                [
                    f"{self.output_column_names[i]} {self.output_types[i]}"
                    for i in range(len(self.output_types))
                ]
            )
        else:
            return self.output_types[0]

    def hopsworksUdf_wrapper(self) -> Callable:
        """
        Function that creates a dynamic wrapper function for the defined udf that renames the columns output by the UDF into specified column names.

        The renames is done so that the column names match the schema expected by spark when multiple columns are returned in a pandas udf.
        The wrapper function would be available in the main scope of the program.

        # Returns
            `Callable`: A wrapper function that renames outputs of the User defined function into specified output column names.
        """
        # Defining wrapper function that renames the column names to specific names
        if len(self.output_types) > 1:
            code = f"""def renaming_wrapper(*args):
    import pandas as pd
    {self._formatted_function_source}
    df = {self.function_name}(*args)
    df = df.rename(columns = {{df.columns[i]: _output_col_names[i] for i in range(len(df.columns))}})
    return df"""
        else:
            code = f"""def renaming_wrapper(*args):
    import pandas as pd
    {self._formatted_function_source}
    df = {self.function_name}(*args)
    df = df.rename(_output_col_names[0])
    return df"""

        # injecting variables into scope used to execute wrapper function.
        scope = __import__("__main__").__dict__
        if self.transformation_statistics is not None:
            scope.update(self.transformation_statistics)
        scope.update({"_output_col_names": self.output_column_names})

        # executing code
        exec(code, scope)

        # returning executed function object
        return eval("renaming_wrapper", scope)

    def __call__(self, *features: List[str]) -> "HopsworksUdf":
        """
        Set features to be passed as arguments to the user defined functions

        # Arguments
            features: Name of features to be passed to the User Defined function
        # Returns
            `HopsworksUdf`: Meta data class for the user defined function.
        # Raises
            `FeatureStoreException: If the provided number of features do not match the number of arguments in the defined UDF or if the provided feature names are not strings.
        """

        if len(features) != len(self.transformation_features):
            raise FeatureStoreException(
                "Number of features provided does not match the number of features provided in the UDF definition"
            )

        for arg in features:
            if not isinstance(arg, str):
                raise FeatureStoreException(
                    f'Feature names provided must be string "{arg}" is not string'
                )
        # Create a copy of the UDF to associate it with new feature names.
        udf = copy.deepcopy(self)

        udf._transformation_features = [
            TransformationFeature(
                new_feature_name, transformation_feature.statistic_argument_name
            )
            for transformation_feature, new_feature_name in zip(
                self._transformation_features, features
            )
        ]
        return udf

    def get_udf(self) -> Callable:
        """
        Function that checks the current engine type and returns the appropriate UDF.

        In the spark engine the UDF is returned as a pandas UDF.
        While in the python engine the UDF is returned as python function.

        # Returns
            `Callable`: Pandas UDF in the spark engine otherwise returns a python function for the UDF.
        """
        if engine.get_type() in ["hive", "python", "training"]:
            return self.hopsworksUdf_wrapper()
        else:
            from pyspark.sql.functions import pandas_udf

            return pandas_udf(
                f=self.hopsworksUdf_wrapper(),
                returnType=self._create_pandas_udf_return_schema_from_list(),
            )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert class into a dictionary for json serialization.

        # Returns
            `Dict`: Dictionary that contains all data required to json serialize the object.
        """
        return {
            "sourceCode": self._original_code,
            "outputTypes": ",".join(
                [python_type.__name__ for python_type in self.output_types]
            )
            if isinstance(self.output_types, List)
            else self.output_types.__name__,
            "transformationFeatures": self.transformation_features,
            "name": self._function_name,
        }

    def json(self) -> str:
        """
        Json serialize object.

        # Returns
            `str`: Json serialized object.
        """
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @classmethod
    def from_response_json(
        cls: "HopsworksUdf", json_dict: Dict[str, Any]
    ) -> "HopsworksUdf":
        """
        Function that deserializes json obtained from the java backend.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
            `HopsworksUdf`: Json deserialized class object.
        """

        json_decamelized = humps.decamelize(json_dict)
        function_source_code = json_decamelized["source_code"]
        function_name = json_decamelized["name"]
        output_types = [
            output_type.strip()
            for output_type in json_decamelized["output_types"].split(",")
        ]
        transformation_features = [
            feature.strip()
            for feature in json_decamelized["transformation_features"].split(",")
        ]

        hopsworks_udf = cls(
            func=function_source_code, output_types=output_types, name=function_name
        )

        # Set transformation features if already set.
        if "" not in transformation_features:
            return hopsworks_udf(*transformation_features)
        else:
            return hopsworks_udf

    @property
    def output_types(self) -> List[str]:
        """Get the output types of the UDF"""
        return self._output_types

    @property
    def function_name(self) -> str:
        """Get the function name of the UDF"""
        return self._function_name

    @property
    def statistics_required(self) -> bool:
        """Get if statistics for any feature is required by the UDF"""
        return bool(self.statistics_features)

    @property
    def transformation_statistics(
        self,
    ) -> Optional[Dict[str, FeatureDescriptiveStatistics]]:
        """Feature statistics required for the defined UDF"""
        return self._statistics

    @property
    def output_column_names(self) -> List[str]:
        """Output columns names of the transformation function"""
        return self._output_column_names

    @property
    def transformation_features(self) -> List[str]:
        """
        List of feature names to be used in the User Defined Function.
        """
        return [
            transformation_feature.feature_name
            for transformation_feature in self._transformation_features
        ]

    @property
    def statistics_features(self) -> List[str]:
        """
        list of feature names that require statistics
        """
        return [
            transformation_feature.feature_name
            for transformation_feature in self._transformation_features
            if transformation_feature.statistic_argument_name is not None
        ]

    @property
    def _statistics_argument_mapping(self) -> Dict[str, str]:
        """
        Dictionary that maps feature names to the statistics arguments names in the User defined function.
        """
        return {
            transformation_feature.feature_name: transformation_feature.statistic_argument_name
            for transformation_feature in self._transformation_features
        }

    @transformation_statistics.setter
    def transformation_statistics(
        self, statistics: List[FeatureDescriptiveStatistics]
    ) -> None:
        self._statistics = dict()
        for stat in statistics:
            if stat.feature_name in self._statistics_argument_mapping.keys():
                self._statistics[
                    self._statistics_argument_mapping[stat.feature_name]
                ] = stat

    @output_column_names.setter
    def output_column_names(self, output_col_names: Union[str, List[str]]) -> None:
        if not isinstance(output_col_names, List):
            output_col_names = [output_col_names]
        if len(output_col_names) != len(self.output_types):
            raise FeatureStoreException(
                f"Provided names for output columns does not match the number of columns returned from the UDF. Please provide {len(self.output_types)} names."
            )
        else:
            self._output_column_names = output_col_names
