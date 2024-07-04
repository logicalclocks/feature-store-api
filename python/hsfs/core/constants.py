import importlib.util


# Data Validation / Great Expectations
HAS_GREAT_EXPECTATIONS: bool = (
    importlib.util.find_spec("great_expectations") is not None
)
great_expectations_not_installed_message = (
    "Great Expectations package not found. "
    "If you want to use data validation with Hopsworks you can install the corresponding extras "
    """`pip install hopsworks[great_expectations]` or `pip install "hopsworks[great_expectations]"` if using zsh. """
    "You can also install great-expectations directly in your environment e.g `pip install great-expectations`. "
    "You will need to restart your kernel if applicable."
)
initialise_expectation_suite_for_single_expectation_api_message = "Initialize Expectation Suite by attaching to a Feature Group to enable single expectation API"

HAS_ARROW: bool = importlib.util.find_spec("pyarrow") is not None
HAS_PANDAS: bool = importlib.util.find_spec("pandas") is not None
HAS_NUMPY: bool = importlib.util.find_spec("numpy") is not None
HAS_POLARS: bool = importlib.util.find_spec("polars") is not None
