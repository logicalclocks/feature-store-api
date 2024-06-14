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
from hsfs.transformation_statistics import TransformationStatistics


def udf(return_type: Union[List[type], type]) -> "HopsworksUdf":
    """
    Create an User Defined Function that can be and used within the Hopsworks Feature Store.

    Hopsworks UDF's are user defined functions that executes as 'pandas_udf' when executing
    in spark engine and as pandas functions in the python engine. The pandas udf/pandas functions
    gets as inputs pandas Series's and can provide as output a pandas Series or a pandas DataFrame.
    A Hopsworks udf is defined using the `hopsworks_udf` decorator. The outputs of the defined UDF
    must be mentioned in the decorator as a list of python types.


    !!! example
        ```python
        from hsfs.hopsworks_udf import udf

       @udf(float)
        def add_one(data1 : pd.Series):
            return data1 + 1
        ```

    # Arguments
        return_type: `list`. The output types of the defined UDF

    # Returns
        `HopsworksUdf`: The metadata object for hopsworks UDF's.

    # Raises
        `hsfs.client.exceptions.FeatureStoreException` : If unable to create UDF.
    """

    def wrapper(func: Callable) -> HopsworksUdf:
        udf = HopsworksUdf(func=func, return_types=return_type)
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
        return_types: Union[List[type], type, List[str], str],
        name: Optional[str] = None,
        transformation_features: Optional[List[TransformationFeature]] = None,
    ):
        self._return_types: List[str] = HopsworksUdf._validate_and_convert_output_types(
            return_types
        )

        self._function_name: str = func.__name__ if name is None else name

        self._function_source: str = (
            HopsworksUdf._extract_source_code(func)
            if isinstance(func, Callable)
            else func
        )
        if not transformation_features:
            self._transformation_features: List[TransformationFeature] = (
                HopsworksUdf._extract_function_arguments(func)
                if not transformation_features
                else transformation_features
            )
        else:
            self._transformation_features = transformation_features

        self._formatted_function_source, self._module_imports = (
            HopsworksUdf._format_source_code(self._function_source)
        )

        self._statistics: Optional[TransformationStatistics] = None

        self._output_column_names: List[str] = self._get_output_column_names()

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
                    f"Output type {output_type} is not supported. Please refer to the documentation to get more information on the supported types."
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
                inspect.getfile(udf_function)
            )
        except FileNotFoundError:
            module_imports = [""]
            warnings.warn(
                "Cannot extract imported dependencies for the function module. Please make sure to import all dependencies for the UDF inside the function.",
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
        arg_list = [
            arg.split(":")[0].split("=")[0].strip()
            for arg in arg_list
            if not arg.strip() == ""
        ]
        if "statistics" in arg_list:
            arg_list.remove("statistics")
        return arg_list, signature, signature_start_line, signature_end_line

    @staticmethod
    def _extract_function_arguments(function: Callable) -> List[TransformationFeature]:
        """
        Function to extract the argument names from a provided function source code.

        # Arguments
            source_code: `Callable`. The function for which the value are to be extracted.
        # Returns
            `List[TransformationFeature]`: List of TransformationFeature that provide a mapping from feature names to corresponding statistics parameters if any is present.
        """
        arg_list = []
        statistics = None
        signature = inspect.signature(function).parameters
        if not signature:
            raise FeatureStoreException(
                "No arguments present in the provided user defined function. Please provide at least one argument in the defined user defined function."
            )
        for arg in inspect.signature(function).parameters.values():
            if arg.name == "statistics":
                statistics = arg.default
            else:
                arg_list.append(arg.name)

        if statistics:
            missing_statistic_features = [
                statistic_feature
                for statistic_feature in statistics._features
                if statistic_feature not in arg_list
            ]
            if missing_statistic_features:
                missing_statistic_features = "', '".join(missing_statistic_features)
                raise FeatureStoreException(
                    f"No argument corresponding to statistics parameter '{missing_statistic_features}' present in function definition."
                )
            return [
                TransformationFeature(arg, arg if arg in statistics._features else None)
                for arg in arg_list
            ]
        else:
            return [TransformationFeature(arg, None) for arg in arg_list]

    @staticmethod
    def _format_source_code(source_code: str) -> Tuple[str, str]:
        """
        Function that parses the existing source code to remove statistics parameter and remove all decorators and type hints from the function source code.

        # Arguments
            source_code: `str`. Source code of a function.
        # Returns
            `Tuple[str, str]`: Tuple that contains Source code that does not contain any decorators, type hints or statistics parameters and the module imports
        """

        arg_list, signature, _, signature_end_line = (
            HopsworksUdf._parse_function_signature(source_code)
        )
        module_imports = source_code.split("@")[0]

        # Reconstruct the function signature
        new_signature = (
            signature.split("(")[0].strip() + "(" + ", ".join(arg_list) + "):"
        )
        source_code = source_code.split("\n")
        # Reconstruct the modified function as a string
        modified_source = (
            new_signature + "\n\t" + "\n\t".join(source_code[signature_end_line + 1 :])
        )

        return modified_source, module_imports

    def _get_output_column_names(self) -> str:
        """
        Function that generates feature names for the transformed features

        # Returns
            `List[str]`: List of feature names for the transformed columns
        """
        _BASE_COLUMN_NAME = (
            f'{self.function_name}_{"-".join(self.transformation_features)}_'
        )
        if len(self.return_types) > 1:
            return [f"{_BASE_COLUMN_NAME}{i}" for i in range(len(self.return_types))]
        else:
            return [f"{_BASE_COLUMN_NAME}"]

    def _create_pandas_udf_return_schema_from_list(self) -> str:
        """
        Function that creates the return schema required for executing the defined UDF's as pandas UDF's in Spark.

        # Returns
            `str`: DDL-formatted type string that denotes the return types of the user defined function.
        """
        if len(self.return_types) > 1:
            return ", ".join(
                [
                    f"`{self.output_column_names[i]}` {self.return_types[i]}"
                    for i in range(len(self.return_types))
                ]
            )
        else:
            return self.return_types[0]

    def hopsworksUdf_wrapper(self) -> Callable:
        """
        Function that creates a dynamic wrapper function for the defined udf that renames the columns output by the UDF into specified column names.

        The renames is done so that the column names match the schema expected by spark when multiple columns are returned in a pandas udf.
        The wrapper function would be available in the main scope of the program.

        # Returns
            `Callable`: A wrapper function that renames outputs of the User defined function into specified output column names.
        """

        # Function to make transformation function time safe. Defined as a string because it has to be dynamically injected into scope to be executed by spark
        convert_timstamp_function = """def convert_timezone(date_time_col : pd.Series):
        import tzlocal
        current_timezone = tzlocal.get_localzone()
        if date_time_col.dt.tz is None:
            # if timestamp is timezone unaware, make sure it's localized to the system's timezone.
            # otherwise, spark will implicitly convert it to the system's timezone.
            return date_time_col.dt.tz_localize(str(current_timezone))
        else:
            # convert to utc, then localize to system's timezone
            return date_time_col.dt.tz_localize(None).dt.tz_localize(str(current_timezone))"""

        # Defining wrapper function that renames the column names to specific names
        if len(self.return_types) > 1:
            code = (
                self._module_imports
                + "\n"
                + f"""import pandas as pd
{convert_timstamp_function}
def renaming_wrapper(*args):
    {self._formatted_function_source}
    df = {self.function_name}(*args)
    df = df.rename(columns = {{df.columns[i]: _output_col_names[i] for i in range(len(df.columns))}})
    for col in df:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = convert_timezone(df[col])
    return df"""
            )
        else:
            code = (
                self._module_imports
                + "\n"
                + f"""import pandas as pd
{convert_timstamp_function}
def renaming_wrapper(*args):
    {self._formatted_function_source}
    df = {self.function_name}(*args)
    df = df.rename(_output_col_names[0])
    if pd.api.types.is_datetime64_any_dtype(df):
        df = convert_timezone(df)
    return df"""
            )

        # injecting variables into scope used to execute wrapper function.

        # Shallow copy of scope performed because updating statistics argument of scope must not affect other instances.
        scope = __import__("__main__").__dict__.copy()
        if self.transformation_statistics is not None:
            scope.update({"statistics": self.transformation_statistics})
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
        udf.output_column_names = udf._get_output_column_names()
        return udf

    def update_return_type_one_hot(self):
        self._return_types = [
            self._return_types[0]
            for _ in range(
                len(
                    self.transformation_statistics[
                        "statistics_feature"
                    ].extended_statistics["unique_values"]
                )
            )
        ]
        self.output_column_names = self._get_output_column_names()

    def get_udf(self, force_python_udf: bool = False) -> Callable:
        """
        Function that checks the current engine type and returns the appropriate UDF.

        In the spark engine the UDF is returned as a pandas UDF.
        While in the python engine the UDF is returned as python function.

        # Arguments
            force_python_udf: `bool`. Force return a python compatible udf irrespective of engine.

        # Returns
            `Callable`: Pandas UDF in the spark engine otherwise returns a python function for the UDF.
        """

        if engine.get_type() in ["hive", "python", "training"] or force_python_udf:
            return self.hopsworksUdf_wrapper()
        else:
            from pyspark.sql.functions import pandas_udf

            return pandas_udf(
                f=self.hopsworksUdf_wrapper(),
                returnType=self._create_pandas_udf_return_schema_from_list(),
            )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert class into a dictionary.

        # Returns
            `Dict`: Dictionary that contains all data required to json serialize the object.
        """
        return {
            "sourceCode": self._function_source,
            "outputTypes": self.return_types,
            "transformationFeatures": self.transformation_features,
            "statisticsArgumentNames": self._statistics_argument_names
            if self.statistics_required
            else None,
            "name": self._function_name,
        }

    def json(self) -> str:
        """
        Convert class into its json serialized form.

        # Returns
            `str`: Json serialized object.
        """
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @classmethod
    def from_response_json(
        cls: "HopsworksUdf", json_dict: Dict[str, Any]
    ) -> "HopsworksUdf":
        """
        Function that constructs the class object from its json serialization.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
            `HopsworksUdf`: Json deserialized class object.
        """

        json_decamelized = humps.decamelize(json_dict)
        function_source_code = json_decamelized["source_code"]
        function_name = json_decamelized["name"]
        output_types = [
            output_type.strip() for output_type in json_decamelized["output_types"]
        ]
        transformation_features = [
            feature.strip() for feature in json_decamelized["transformation_features"]
        ]
        statistics_features = (
            [
                feature.strip()
                for feature in json_decamelized["statistics_argument_names"]
            ]
            if "statistics_argument_names" in json_decamelized
            else None
        )

        # Reconstructing statistics arguments.
        arg_list, _, _, _ = HopsworksUdf._parse_function_signature(function_source_code)

        transformation_features = (
            arg_list if not transformation_features else transformation_features
        )

        if statistics_features:
            transformation_features = [
                TransformationFeature(
                    transformation_features[arg_index],
                    arg_list[arg_index]
                    if arg_list[arg_index] in statistics_features
                    else None,
                )
                for arg_index in range(len(arg_list))
            ]
        else:
            transformation_features = [
                TransformationFeature(transformation_features[arg_index], None)
                for arg_index in range(len(arg_list))
            ]

        hopsworks_udf = cls(
            func=function_source_code,
            return_types=output_types,
            name=function_name,
            transformation_features=transformation_features,
        )

        # Set transformation features if already set.
        return hopsworks_udf

    @property
    def return_types(self) -> List[str]:
        """Get the output types of the UDF"""
        # Update the number of outputs for one hot encoder to match the number of unique values for the feature
        if self.function_name == "one_hot_encoder" and self.transformation_statistics:
            self.update_return_type_one_hot()
        return self._return_types

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
    ) -> Optional[TransformationStatistics]:
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

    @property
    def _statistics_argument_names(self) -> List[str]:
        """
        list of argument names required for statistics
        """
        return [
            transformation_feature.statistic_argument_name
            for transformation_feature in self._transformation_features
            if transformation_feature.statistic_argument_name is not None
        ]

    @transformation_statistics.setter
    def transformation_statistics(
        self, statistics: List[FeatureDescriptiveStatistics]
    ) -> None:
        self._statistics = TransformationStatistics(*self._statistics_argument_names)
        for stat in statistics:
            if stat.feature_name in self._statistics_argument_mapping.keys():
                self._statistics.set_statistics(
                    self._statistics_argument_mapping[stat.feature_name], stat.to_dict()
                )

    @output_column_names.setter
    def output_column_names(self, output_col_names: Union[str, List[str]]) -> None:
        if not isinstance(output_col_names, List):
            output_col_names = [output_col_names]
        if len(output_col_names) != len(self.return_types):
            raise FeatureStoreException(
                f"Provided names for output columns does not match the number of columns returned from the UDF. Please provide {len(self.return_types)} names."
            )
        else:
            self._output_column_names = output_col_names
