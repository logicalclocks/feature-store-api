import pandas as pd
from hsfs.statistics import FeatureDescriptiveStatistics


def test_function():
    return True


def test_function_one_argument(arg1):
    pass


def test_function_one_argument_with_statistics(arg1, statistics_arg1):
    pass


def test_function_one_argument_with_typehints(arg1: pd.Series):
    pass


def test_function_one_argument_with_statistics_and_typehints(
    arg1: pd.Series, statistics_arg1: FeatureDescriptiveStatistics
):
    pass


def test_function_multiple_argument(arg1, arg2):
    pass


def test_function_multiple_argument_with_statistics(
    arg1, arg2, arg3, statistics_arg1, statistics_arg3
):
    pass


def test_function_multiple_argument_with_typehints(arg1: pd.Series, arg2: pd.Series):
    pass


def test_function_multiple_argument_with_statistics_and_typehints(
    arg1: pd.Series,
    arg2: pd.Series,
    statistics_arg1: FeatureDescriptiveStatistics,
    statistics_arg2: FeatureDescriptiveStatistics,
):
    pass


def test_function_multiple_argument_with_mixed_statistics_and_typehints(
    arg1: pd.Series,
    arg2,
    arg3,
    statistics_arg1,
    statistics_arg3: FeatureDescriptiveStatistics,
):
    pass


def test_function_multiple_argument_all_parameter_with_spaces(
    arg1: pd.Series,
    arg2,
    statistics_arg1,
    statistics_arg2: FeatureDescriptiveStatistics,
):
    pass


def test_function_multiple_argument_all_parameter_multiline(
    arg1: pd.Series,
    arg2,
    statistics_arg1,
    arg3,
    statistics_arg3: FeatureDescriptiveStatistics,
):
    pass


def test_function_multiple_argument_all_parameter_multiline_with_comments(
    arg1: pd.Series,  # Test Comment
    arg2,
    statistics_arg1,  # Test Comment
    arg3,
    statistics_arg3: FeatureDescriptiveStatistics,
) -> pd.DataFrame:  # Test Comment
    pass


def test_function_statistics_invalid(
    arg1: pd.Series, statistics_arg3: FeatureDescriptiveStatistics
):
    pass
