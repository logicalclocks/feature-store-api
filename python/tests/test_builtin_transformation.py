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

import hsfs.engine as engine
import pandas as pd
from hsfs.builtin_transformations import (
    min_max_scaler,
)
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.engine import python
from hsfs.hopsworks_udf import UDFType


class TestBuiltinTransformations:
    @staticmethod
    def validate_transformations_python(
        transformed_outputs, expected_output, expected_col_names
    ):
        if isinstance(transformed_outputs, pd.Series):
            assert transformed_outputs.name == expected_col_names
        else:
            assert all(transformed_outputs.columns == expected_col_names)
        assert all(transformed_outputs.values == expected_output.values)

    def test_min_max_scaler(self):
        test_dataframe = pd.DataFrame(
            {
                "col1": [1, 2, 3, 4],
                "col2": [1.2, 3.4, 5.6, 9.1],
            }
        )
        statistics_df = test_dataframe.describe().to_dict()

        # Test case 1 : Integer column
        min_max_scaler_col1 = min_max_scaler("col1")
        min_max_scaler_col1.udf_type = UDFType.MODEL_DEPENDENT

        min_max_scaler_col1.transformation_statistics = [
            FeatureDescriptiveStatistics(
                feature_name="col1",
                min=statistics_df["col1"]["min"],
                max=statistics_df["col1"]["max"],
            )
        ]

        expected_df = (test_dataframe["col1"] - test_dataframe["col1"].min()) / (
            test_dataframe["col1"].max() - test_dataframe["col1"].min()
        )

        # Test with python engine
        engine.set_instance(engine=python.Engine(), engine_type="python")

        transformed_df = min_max_scaler_col1.get_udf()(test_dataframe["col1"])
        TestBuiltinTransformations.validate_transformations_python(
            transformed_outputs=transformed_df,
            expected_output=expected_df,
            expected_col_names="min_max_scaler_col1_",
        )

        # Test with spark engine
        engine.set_instance(engine=python.Engine(), engine_type="python")

        transformed_df = min_max_scaler_col1.get_udf()(test_dataframe["col1"])
        TestBuiltinTransformations.validate_transformations_python(
            transformed_outputs=transformed_df,
            expected_output=expected_df,
            expected_col_names="min_max_scaler_col1_",
        )
