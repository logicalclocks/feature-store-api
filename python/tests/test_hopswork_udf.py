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

from datetime import date, datetime, time

import pandas as pd
import pytest
from hsfs.client.exceptions import FeatureStoreException
from hsfs.hopsworks_udf import HopsworksUdf, TransformationFeature, UDFType, udf


class TestHopsworksUdf:
    def test_validate_and_convert_output_types_one_elements(self):
        assert HopsworksUdf._validate_and_convert_output_types([int]) == ["bigint"]

        assert HopsworksUdf._validate_and_convert_output_types([float]) == ["double"]

        assert HopsworksUdf._validate_and_convert_output_types([str]) == ["string"]

        assert HopsworksUdf._validate_and_convert_output_types([bool]) == ["boolean"]

        assert HopsworksUdf._validate_and_convert_output_types([datetime]) == [
            "timestamp"
        ]

        assert HopsworksUdf._validate_and_convert_output_types([time]) == ["timestamp"]

        assert HopsworksUdf._validate_and_convert_output_types([date]) == ["date"]

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([pd.DatetimeTZDtype])

        assert (
            str(exception.value)
            == f"Output type {pd.DatetimeTZDtype} is not supported. Please refer to the documentation to get more information on the supported types."
        )

    def test_validate_and_convert_output_types_multiple_types(self):
        assert HopsworksUdf._validate_and_convert_output_types(
            [int, float, str, bool, datetime, date, time]
        ) == ["bigint", "double", "string", "boolean", "timestamp", "date", "timestamp"]

        assert HopsworksUdf._validate_and_convert_output_types(
            ["bigint", "double", "string", "boolean", "timestamp", "date"]
        ) == ["bigint", "double", "string", "boolean", "timestamp", "date"]

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([pd.DatetimeTZDtype])

        assert (
            str(exception.value)
            == f"Output type {pd.DatetimeTZDtype} is not supported. Please refer to the documentation to get more information on the supported types."
        )

    def test_validate_and_convert_output_types_invalid_types(self):
        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([pd.DatetimeTZDtype])

        assert (
            str(exception.value)
            == f"Output type {pd.DatetimeTZDtype} is not supported. Please refer to the documentation to get more information on the supported types."
        )

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([int, pd.DatetimeTZDtype])

        assert (
            str(exception.value)
            == f"Output type {pd.DatetimeTZDtype} is not supported. Please refer to the documentation to get more information on the supported types."
        )

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([int, "pd.DatetimeTZDtype"])

        assert (
            str(exception.value)
            == "Output type pd.DatetimeTZDtype is not supported. Please refer to the documentation to get more information on the supported types."
        )

    def test_get_module_imports(self):
        assert HopsworksUdf._get_module_imports(
            "python/tests/test_helpers/transformation_test_helper.py"
        ) == [
            "import pandas as pd",
            "from hsfs.transformation_statistics import TransformationStatistics",
        ]

    def test_extract_source_code(self):
        from test_helpers.transformation_test_helper import test_function

        assert """import pandas as pd
from hsfs.transformation_statistics import TransformationStatistics
def test_function():
    return True""" == HopsworksUdf._extract_source_code(test_function).strip()

    def test_extract_function_arguments_no_arguments(self):
        from test_helpers.transformation_test_helper import test_function

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._extract_function_arguments(test_function)

        assert (
            str(exception.value)
            == "No arguments present in the provided user defined function. Please provide at least one argument in the defined user defined function."
        )

    def test_extract_function_arguments_one_argument(self):
        from test_helpers.transformation_test_helper import test_function_one_argument

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_one_argument
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None)
        ]

    def test_extract_function_arguments_one_argument_with_statistics(self):
        from test_helpers.transformation_test_helper import (
            test_function_one_argument_with_statistics,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_one_argument_with_statistics
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1")
        ]

    def test_extract_function_arguments_one_argument_with_typehint(self):
        from test_helpers.transformation_test_helper import (
            test_function_one_argument_with_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_one_argument_with_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None)
        ]

    def test_extract_function_arguments_one_argument_with_statistics_and_typehints(
        self,
    ):
        from test_helpers.transformation_test_helper import (
            test_function_one_argument_with_statistics_and_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_one_argument_with_statistics_and_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1")
        ]

    def test_extract_function_arguments_multiple_argument(self):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
        ]

    def test_extract_function_arguments_multiple_argument_with_statistics(self):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_statistics,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_with_statistics
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
            TransformationFeature(feature_name="arg3", statistic_argument_name="arg3"),
        ]

    def test_extract_function_arguments_multiple_argument_with_typehints(self):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_with_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
        ]

    def test_extract_function_arguments_multiple_argument_with_statistics_and_typehints(
        self,
    ):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_statistics_and_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_with_statistics_and_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name="arg2"),
        ]

    def test_extract_function_arguments_multiple_argument_with_mixed_statistics_and_typehints(
        self,
    ):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_mixed_statistics_and_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_with_mixed_statistics_and_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
            TransformationFeature(feature_name="arg3", statistic_argument_name="arg3"),
        ]

    def test_extract_function_arguments_multiple_argument_all_parameter_with_spaces(
        self,
    ):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_with_spaces,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_all_parameter_with_spaces
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name="arg2"),
        ]

    def test_extract_function_arguments_multiple_argument_all_parameter_multiline(self):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_multiline,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_all_parameter_multiline
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
            TransformationFeature(feature_name="arg3", statistic_argument_name="arg3"),
        ]

    def test_extract_function_arguments_multiple_argumen_all_parameter_multiline_with_comments(
        self,
    ):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_multiline_with_comments,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_all_parameter_multiline_with_comments
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
            TransformationFeature(feature_name="arg3", statistic_argument_name="arg3"),
        ]

    def test_extract_function_arguments_statistics_invalid(self):
        from test_helpers.transformation_test_helper import (
            test_function_statistics_invalid,
        )

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._extract_function_arguments(test_function_statistics_invalid)

        assert (
            str(exception.value)
            == "No argument corresponding to statistics parameter 'arg3' present in function definition."
        )

    def test_format_source_code(self):
        from test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_multiline_with_comments,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument_all_parameter_multiline_with_comments
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument_all_parameter_multiline_with_comments(arg1, arg2, arg3):
\t    pass"""
        )

    def test_generate_output_column_names_one_argument_one_output_type(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        test_func.udf_type = UDFType.MODEL_DEPENDENT
        assert test_func._get_output_column_names() == ["test_func_col1_"]

        test_func.udf_type = UDFType.ON_DEMAND
        assert test_func._get_output_column_names() == ["test_func"]

    def test_generate_output_column_names_one_argument_one_output_type_prefix(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        test_func._feature_name_prefix = "prefix_"

        test_func.udf_type = UDFType.MODEL_DEPENDENT
        assert test_func._get_output_column_names() == ["test_func_prefix_col1_"]
        assert test_func.output_column_names == ["prefix_test_func_prefix_col1_"]

        test_func.udf_type = UDFType.ON_DEMAND
        assert test_func._get_output_column_names() == ["test_func"]
        assert test_func.output_column_names == ["prefix_test_func"]

    def test_generate_output_column_names_multiple_argument_one_output_type(self):
        @udf(int)
        def test_func(col1, col2, col3):
            return col1 + 1

        test_func.udf_type = UDFType.MODEL_DEPENDENT
        assert test_func._get_output_column_names() == ["test_func_col1_col2_col3_"]
        test_func.udf_type = UDFType.ON_DEMAND
        assert test_func._get_output_column_names() == ["test_func"]

    def test_generate_output_column_names_multiple_argument_one_output_type_prefix(
        self,
    ):
        @udf(int)
        def test_func(col1, col2, col3):
            return col1 + 1

        test_func._feature_name_prefix = "prefix_"

        test_func.udf_type = UDFType.MODEL_DEPENDENT
        assert test_func._get_output_column_names() == [
            "test_func_prefix_col1_prefix_col2_prefix_col3_"
        ]
        assert test_func.output_column_names == [
            "prefix_test_func_prefix_col1_prefix_col2_prefix_col3_"
        ]
        test_func.udf_type = UDFType.ON_DEMAND
        assert test_func._get_output_column_names() == ["test_func"]
        assert test_func.output_column_names == ["prefix_test_func"]

    def test_generate_output_column_names_single_argument_multiple_output_type(self):
        @udf([int, float, int])
        def test_func(col1):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col1 + 1], "col3": [col1 + 1]}
            )

        test_func.udf_type = UDFType.MODEL_DEPENDENT
        assert test_func._get_output_column_names() == [
            "test_func_col1_0",
            "test_func_col1_1",
            "test_func_col1_2",
        ]

    def test_generate_output_column_names_single_argument_multiple_output_type_prefix(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col1 + 1], "col3": [col1 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"

        test_func.udf_type = UDFType.MODEL_DEPENDENT
        assert test_func._get_output_column_names() == [
            "test_func_prefix_col1_0",
            "test_func_prefix_col1_1",
            "test_func_prefix_col1_2",
        ]
        assert test_func.output_column_names == [
            "prefix_test_func_prefix_col1_0",
            "prefix_test_func_prefix_col1_1",
            "prefix_test_func_prefix_col1_2",
        ]

    def test_generate_output_column_names_multiple_argument_multiple_output_type(self):
        @udf([int, float, int])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func.udf_type = UDFType.MODEL_DEPENDENT
        assert test_func._get_output_column_names() == [
            "test_func_col1_col2_col3_0",
            "test_func_col1_col2_col3_1",
            "test_func_col1_col2_col3_2",
        ]

    def test_generate_output_column_names_multiple_argument_multiple_output_type_prefix(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"

        test_func.udf_type = UDFType.MODEL_DEPENDENT
        assert test_func._get_output_column_names() == [
            "test_func_prefix_col1_prefix_col2_prefix_col3_0",
            "test_func_prefix_col1_prefix_col2_prefix_col3_1",
            "test_func_prefix_col1_prefix_col2_prefix_col3_2",
        ]
        assert test_func.output_column_names == [
            "prefix_test_func_prefix_col1_prefix_col2_prefix_col3_0",
            "prefix_test_func_prefix_col1_prefix_col2_prefix_col3_1",
            "prefix_test_func_prefix_col1_prefix_col2_prefix_col3_2",
        ]

    def test_drop_features_one_element(self):
        @udf([int, float, int], drop="col1")
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func.udf_type = UDFType.MODEL_DEPENDENT

        assert test_func.dropped_features == ["col1"]

    def test_drop_features_one_element_prefix(self):
        @udf([int, float, int], drop="col1")
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"
        test_func.udf_type = UDFType.MODEL_DEPENDENT

        assert test_func._dropped_features == ["col1"]
        assert test_func.dropped_features == ["prefix_col1"]

    def test_drop_features_multiple_element(self):
        @udf([int, float, int], drop=["col1", "col2"])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func.udf_type = UDFType.MODEL_DEPENDENT

        assert test_func.dropped_features == ["col1", "col2"]

    def test_drop_features_multiple_element_prefix(self):
        @udf([int, float, int], drop=["col1", "col2"])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"
        test_func.udf_type = UDFType.MODEL_DEPENDENT

        assert test_func._dropped_features == ["col1", "col2"]
        assert test_func.dropped_features == ["prefix_col1", "prefix_col2"]

    def test_drop_features_invalid(self):
        with pytest.raises(FeatureStoreException) as exp:

            @udf([int, float, int], drop=["col1", "invalid_col"])
            def test_func(col1, col2, col3):
                return pd.DataFrame(
                    {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
                )

        assert (
            str(exp.value)
            == "Cannot drop features 'invalid_col' as they are not features given as arguments in the defined UDF."
        )

    def test_create_pandas_udf_return_schema_from_list_one_output_type(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        assert test_func._create_pandas_udf_return_schema_from_list() == "bigint"

    def test_create_pandas_udf_return_schema_from_list_one_argument_multiple_output_type(
        self,
    ):
        @udf([int, float, str, date, datetime, time, bool])
        def test_func(col1):
            return pd.DataFrame(
                {
                    "col1": [col1 + 1],
                    "col2": [col1 + 1],
                    "col3": [col1 + 1],
                    "col4": [col1 + 1],
                    "col5": [col1 + 1],
                    "col6": [True],
                }
            )

        test_func.udf_type = UDFType.MODEL_DEPENDENT

        assert (
            test_func._create_pandas_udf_return_schema_from_list()
            == "`test_func_col1_0` bigint, `test_func_col1_1` double, `test_func_col1_2` string, `test_func_col1_3` date, `test_func_col1_4` timestamp, `test_func_col1_5` timestamp, `test_func_col1_6` boolean"
        )

    def test_hopsworks_wrapper_single_output(self):
        test_dataframe = pd.DataFrame({"col1": [1, 2, 3, 4]})

        @udf(int)
        def test_func(col1):
            return col1 + 1

        test_func.udf_type = UDFType.MODEL_DEPENDENT

        renaming_wrapper_function = test_func.hopsworksUdf_wrapper()

        result = renaming_wrapper_function(test_dataframe["col1"])

        assert result.name == "test_func_col1_"
        assert result.values.tolist() == [2, 3, 4, 5]

        test_func.udf_type = UDFType.ON_DEMAND

        renaming_wrapper_function = test_func.hopsworksUdf_wrapper()

        result = renaming_wrapper_function(test_dataframe["col1"])

        assert result.name == "test_func"
        assert result.values.tolist() == [2, 3, 4, 5]

    def test_hopsworks_wrapper_multiple_output(self):
        @udf([int, float])
        def test_func(col1, col2):
            return pd.DataFrame({"out1": col1 + 1, "out2": col2 + 2})

        test_func.udf_type = UDFType.MODEL_DEPENDENT

        renaming_wrapper_function = test_func.hopsworksUdf_wrapper()

        test_dataframe = pd.DataFrame(
            {"column1": [1, 2, 3, 4], "column2": [10, 20, 30, 40]}
        )

        result = renaming_wrapper_function(
            test_dataframe["column1"], test_dataframe["column2"]
        )

        assert all(result.columns == ["test_func_col1_col2_0", "test_func_col1_col2_1"])
        assert result.values.tolist() == [[2, 12], [3, 22], [4, 32], [5, 42]]

    def test_HopsworkUDf_call_one_argument(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        assert test_func.transformation_features == ["col1"]
        assert test_func.statistics_features == []

        assert test_func("new_feature").transformation_features == ["new_feature"]
        assert test_func("new_feature").statistics_features == []

        # Test with prefix
        test_func._feature_name_prefix = "prefix_"
        assert test_func.transformation_features == ["prefix_col1"]
        assert test_func.statistics_features == []

        assert test_func("new_feature").transformation_features == [
            "prefix_new_feature"
        ]
        assert test_func("new_feature").statistics_features == []

    def test_HopsworkUDf_call_one_argument_statistics(self):
        from hsfs.transformation_statistics import TransformationStatistics

        stats = TransformationStatistics("col1")

        @udf(int)
        def test_func(col1, statistics=stats):
            return col1 + statistics.col1.mean

        assert test_func.transformation_features == ["col1"]
        assert test_func.statistics_features == ["col1"]
        assert test_func._statistics_argument_names == ["col1"]

        assert test_func("new_feature").transformation_features == ["new_feature"]
        assert test_func("new_feature").statistics_features == ["new_feature"]
        assert test_func("new_feature")._statistics_argument_names == ["col1"]

        # Test with prefix
        test_func._feature_name_prefix = "prefix_"
        assert test_func.transformation_features == ["prefix_col1"]
        assert test_func.statistics_features == ["col1"]
        assert test_func._statistics_argument_names == ["col1"]

        assert test_func("new_feature").transformation_features == [
            "prefix_new_feature"
        ]
        assert test_func("new_feature").statistics_features == ["new_feature"]
        assert test_func("new_feature")._statistics_argument_names == ["col1"]

    def test_HopsworkUDf_call_multiple_argument_statistics(self):
        from hsfs.transformation_statistics import TransformationStatistics

        stats = TransformationStatistics("col1", "col3")

        @udf(int)
        def test_func(col1, col2, col3, statistics=stats):
            return col1 + statistics.col1.mean + statistics.col3.mean

        assert test_func.transformation_features == ["col1", "col2", "col3"]
        assert test_func.statistics_features == ["col1", "col3"]

        assert test_func("f1", "f2", "f3").transformation_features == ["f1", "f2", "f3"]
        assert test_func("f1", "f2", "f3").statistics_features == ["f1", "f3"]
        assert test_func("f1", "f2", "f3")._statistics_argument_names == [
            "col1",
            "col3",
        ]

    def test_validate_and_convert_drop_features(self):
        dropped_features = "feature1"
        transformation_feature = ["feature1", "feature2"]
        feature_name_prefix = None

        dropped_features = HopsworksUdf._validate_and_convert_drop_features(
            dropped_features, transformation_feature, feature_name_prefix
        )

        assert dropped_features == ["feature1"]

    def test_validate_and_convert_drop_features_dropped_list(self):
        dropped_features = ["feature1", "feature2"]
        transformation_feature = ["feature1", "feature2", "feature3"]
        feature_name_prefix = None

        dropped_features = HopsworksUdf._validate_and_convert_drop_features(
            dropped_features, transformation_feature, feature_name_prefix
        )

        assert dropped_features == ["feature1", "feature2"]

    def test_validate_and_convert_drop_features_dropped_invalid(self):
        dropped_features = "feature4"
        transformation_feature = ["feature1", "feature2", "feature3"]
        feature_name_prefix = None

        with pytest.raises(FeatureStoreException) as exp:
            HopsworksUdf._validate_and_convert_drop_features(
                dropped_features, transformation_feature, feature_name_prefix
            )

        assert (
            str(exp.value)
            == "Cannot drop features 'feature4' as they are not features given as arguments in the defined UDF."
        )

    def test_validate_and_convert_drop_features_dropped_invalid_list(self):
        dropped_features = ["feature4", "feature5"]
        transformation_feature = ["feature1", "feature2", "feature3"]
        feature_name_prefix = None

        with pytest.raises(FeatureStoreException) as exp:
            HopsworksUdf._validate_and_convert_drop_features(
                dropped_features, transformation_feature, feature_name_prefix
            )

        assert (
            str(exp.value)
            == "Cannot drop features 'feature4', 'feature5' as they are not features given as arguments in the defined UDF."
        )

    def test_validate_and_convert_drop_features_dropped_list_prefix(self):
        dropped_features = ["feature1", "feature2"]
        transformation_feature = ["test_feature1", "test_feature2", "test_feature3"]
        feature_name_prefix = "test_"

        dropped_features = HopsworksUdf._validate_and_convert_drop_features(
            dropped_features, transformation_feature, feature_name_prefix
        )

        assert dropped_features == ["feature1", "feature2"]

    def test_validate_and_convert_drop_features_dropped_prefix_invalid(self):
        dropped_features = ["feature1", "feature2"]
        transformation_feature = ["feature1", "feature2", "feature3"]
        feature_name_prefix = "test_"

        with pytest.raises(FeatureStoreException) as exp:
            HopsworksUdf._validate_and_convert_drop_features(
                dropped_features, transformation_feature, feature_name_prefix
            )

        assert (
            str(exp.value)
            == "Cannot drop features 'test_feature1', 'test_feature2' as they are not features given as arguments in the defined UDF."
        )

    def test_validate_udf_type_None(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        with pytest.raises(FeatureStoreException) as exe:
            test_func._validate_udf_type()
            test_func.get_udf()

        assert str(exe.value) == "UDF Type cannot be None"

    def test_validate_udf_type_on_demand_multiple_output(self):
        @udf([int, float])
        def test_func(col1, col2):
            return pd.DataFrame({"out1": col1 + 1, "out2": col2 + 2})

        with pytest.raises(FeatureStoreException) as exe:
            test_func.udf_type = UDFType.ON_DEMAND

        assert (
            str(exe.value)
            == "On-Demand Transformation functions can only return one column as output"
        )

    def test_validate_udf_type_on_demand_statistics(self):
        from hsfs.transformation_statistics import TransformationStatistics

        stats = TransformationStatistics("col1")

        @udf(int)
        def test_func(col1, statistics=stats):
            return col1 + statistics.col1.mean

        with pytest.raises(FeatureStoreException) as exe:
            test_func.udf_type = UDFType.ON_DEMAND

        assert (
            str(exe.value)
            == "On-Demand Transformation functions cannot use statistics, please remove statistics parameters from the functions"
        )
