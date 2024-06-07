import pandas as pd
from hsfs.transformation_statistics import TransformationStatistics


stats_arg1 = TransformationStatistics("arg1")
stats_arg1_arg3 = TransformationStatistics("arg1", "arg3")
stats_arg1_arg2 = TransformationStatistics("arg1", "arg2")
stats_arg3 = TransformationStatistics("arg3")


def test_function():
    return True


def test_function_one_argument(arg1):
    pass


def test_function_one_argument_with_statistics(arg1, statistics=stats_arg1):
    pass


def test_function_one_argument_with_typehints(arg1: pd.Series):
    pass


def test_function_one_argument_with_statistics_and_typehints(
    arg1: pd.Series, statistics=stats_arg1
):
    pass


def test_function_multiple_argument(arg1, arg2):
    pass


def test_function_multiple_argument_with_statistics(
    arg1, arg2, arg3, statistics=stats_arg1_arg3
):
    pass


def test_function_multiple_argument_with_typehints(arg1: pd.Series, arg2: pd.Series):
    pass


def test_function_multiple_argument_with_statistics_and_typehints(
    arg1: pd.Series, arg2: pd.Series, statistics=stats_arg1_arg2
):
    pass


def test_function_multiple_argument_with_mixed_statistics_and_typehints(
    arg1: pd.Series, arg2, arg3, statistics=stats_arg1_arg3
):
    pass


def test_function_multiple_argument_all_parameter_with_spaces(
    arg1: pd.Series, arg2, statistics=stats_arg1_arg2
):
    pass


def test_function_multiple_argument_all_parameter_multiline(
    arg1: pd.Series, arg2, arg3, statistics=stats_arg1_arg3
):
    pass


def test_function_multiple_argument_all_parameter_multiline_with_comments(
    arg1: pd.Series,  # Test Comment
    arg2,
    arg3,  # Test Comment
    statistics=stats_arg1_arg3,  # Test Comment
) -> pd.DataFrame:  # Test Comment
    pass


def test_function_statistics_invalid(arg1: pd.Series, statistics=stats_arg3):
    pass
