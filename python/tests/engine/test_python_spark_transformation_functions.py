#
#   Copyright 2022 Hopsworks AB
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
from __future__ import annotations

import datetime
import os
import statistics

import pandas as pd
import pytest
import tzlocal
from hsfs import (
    engine,
    training_dataset,
    training_dataset_feature,
    transformation_function,
)
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.engine import python, spark
from hsfs.hopsworks_udf import HopsworksUdf, UDFType, udf
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# TODO : Remove skipping UT in windows after Greater expectations has been upgraded to 1.0 or after it has been made optional
@pytest.mark.skipif(
    os.name == "nt",
    reason="Skip tests in windows since it fails due to dependency problem with greater expectations 0.18.2, Fixed on upgrading to 1.0",
)
class TestPythonSparkTransformationFunctions:
    def _create_training_dataset(self):
        f = training_dataset_feature.TrainingDatasetFeature(
            name="col_0", type=IntegerType(), index=0
        )
        f1 = training_dataset_feature.TrainingDatasetFeature(
            name="col_1", type=StringType(), index=1
        )
        f2 = training_dataset_feature.TrainingDatasetFeature(
            name="col_2", type=BooleanType(), index=1
        )
        features = [f, f1, f2]

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            features=features,
        )

        return td

    def _validate_on_python_engine(self, td, df, expected_df, transformation_functions):
        # Arrange
        engine._engine_type = "python"
        python_engine = python.Engine()

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=transformation_functions,
            dataset=df,
        )

        assert list(result.columns) == list(expected_df.columns)
        assert list(result.dtypes) == list(expected_df.dtypes)
        assert result.equals(expected_df)

    def _validate_on_spark_engine(
        self, td, spark_df, expected_spark_df, transformation_functions
    ):
        # Arrange
        engine._engine_type = "spark"
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=transformation_functions,
            dataset=spark_df,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_builtin_minmax_from_backend(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.core.statistics_engine.StatisticsEngine._save_statistics")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("min_max_scaler_col_0_", DoubleType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "min_max_scaler_col_0_": [0.0, 1.0],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        tf_fun_source = (
            "import numpy as np\nimport pandas as pd\nfrom hsfs.transformation_statistics import TransformationStatistics\n"
            "from hsfs.hopsworks_udf import udf\n"
            'feature_statistics = TransformationStatistics("feature")\n'
            "@udf(float)\n"
            "def min_max_scaler(feature: pd.Series, statistics = feature_statistics) -> pd.Series:\n"
            "    return (feature - statistics.feature.min) / (statistics.feature.max - statistics.feature.min)"
        )
        udf_response = {
            "sourceCode": tf_fun_source,
            "outputTypes": ["double"],
            "transformationFeatures": [],
            "statisticsArgumentNames": ["feature"],
            "name": "min_max_scaler",
            "droppedArgumentNames": ["feature"],
        }

        tf_fun = HopsworksUdf.from_response_json(udf_response)

        td = self._create_training_dataset()

        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun("col_0"),
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        transformation_functions[0].hopsworks_udf.transformation_statistics = [
            FeatureDescriptiveStatistics(feature_name="col_0", min=1, max=2)
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_builtin_minmax(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.core.statistics_engine.StatisticsEngine._save_statistics")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("min_max_scaler_col_0_", DoubleType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "min_max_scaler_col_0_": [0.0, 1.0],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        from hsfs.builtin_transformations import min_max_scaler

        td = self._create_training_dataset()

        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=min_max_scaler("col_0"),
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        transformation_functions[0].hopsworks_udf.transformation_statistics = [
            FeatureDescriptiveStatistics(feature_name="col_0", min=1, max=2)
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_builtin_standard_scaler_from_backend(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.core.statistics_engine.StatisticsEngine._save_statistics")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("standard_scaler_col_0_", DoubleType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "standard_scaler_col_0_": [-1.0, 1.0],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        tf_fun_source = (
            "import numpy as np\nimport pandas as pd\nfrom hsfs.transformation_statistics import TransformationStatistics\n"
            "from hsfs.hopsworks_udf import udf\n"
            'feature_statistics = TransformationStatistics("feature")\n'
            "@udf(float)\n"
            "def standard_scaler(feature: pd.Series, statistics = feature_statistics) -> pd.Series:\n"
            "    return (feature - statistics.feature.mean) / statistics.feature.stddev"
        )
        udf_response = {
            "sourceCode": tf_fun_source,
            "outputTypes": ["double"],
            "transformationFeatures": [],
            "statisticsArgumentNames": ["feature"],
            "name": "standard_scaler",
            "droppedArgumentNames": ["feature"],
        }

        tf_fun = HopsworksUdf.from_response_json(udf_response)

        td = self._create_training_dataset()

        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun("col_0"),
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]
        mean = statistics.mean([1, 2])
        stddev = statistics.pstdev([1, 2])
        transformation_functions[0].hopsworks_udf.transformation_statistics = [
            FeatureDescriptiveStatistics(feature_name="col_0", mean=mean, stddev=stddev)
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_builtin_standard_scaler(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.core.statistics_engine.StatisticsEngine._save_statistics")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("standard_scaler_col_0_", DoubleType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "standard_scaler_col_0_": [-1.0, 1.0],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        from hsfs.builtin_transformations import standard_scaler

        td = self._create_training_dataset()

        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=standard_scaler("col_0"),
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        mean = statistics.mean([1, 2])
        stddev = statistics.pstdev([1, 2])
        transformation_functions[0].hopsworks_udf.transformation_statistics = [
            FeatureDescriptiveStatistics(feature_name="col_0", mean=mean, stddev=stddev)
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_builtin_robust_scaler_from_backend(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.core.statistics_engine.StatisticsEngine._save_statistics")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("robust_scaler_col_0_", DoubleType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "robust_scaler_col_0_": [-1.0, 0],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        tf_fun_source = (
            "import numpy as np\nimport pandas as pd\nfrom hsfs.transformation_statistics import TransformationStatistics\n"
            "from hsfs.hopsworks_udf import udf\n"
            'feature_statistics = TransformationStatistics("feature")\n'
            "@udf(float)\n"
            "def robust_scaler(feature: pd.Series, statistics = feature_statistics) -> pd.Series:\n"
            "    return (feature - statistics.feature.percentiles[49]) / (statistics.feature.percentiles[74] - "
            "statistics.feature.percentiles[24])"
        )
        udf_response = {
            "sourceCode": tf_fun_source,
            "outputTypes": ["double"],
            "transformationFeatures": [],
            "statisticsArgumentNames": ["feature"],
            "name": "robust_scaler",
            "droppedArgumentNames": ["feature"],
        }

        tf_fun = HopsworksUdf.from_response_json(udf_response)

        td = self._create_training_dataset()

        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun("col_0"),
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]
        percentiles = [1] * 100
        percentiles[24] = 1
        percentiles[49] = 2
        percentiles[74] = 2
        transformation_functions[0].hopsworks_udf.transformation_statistics = [
            FeatureDescriptiveStatistics(feature_name="col_0", percentiles=percentiles)
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_builtin_robust_scaler(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.core.statistics_engine.StatisticsEngine._save_statistics")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("robust_scaler_col_0_", DoubleType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "robust_scaler_col_0_": [-1.0, 0],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        from hsfs.builtin_transformations import robust_scaler

        td = self._create_training_dataset()

        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=robust_scaler("col_0"),
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        percentiles = [1] * 100
        percentiles[24] = 1
        percentiles[49] = 2
        percentiles[74] = 2
        transformation_functions[0].hopsworks_udf.transformation_statistics = [
            FeatureDescriptiveStatistics(feature_name="col_0", percentiles=percentiles)
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_int(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("tf_fun_col_0_", LongType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "tf_fun_col_0_": [2, 3],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        @udf(int, drop=["col_0"])
        def tf_fun(col_0):
            return col_0 + 1

        td = self._create_training_dataset()

        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun,
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_str(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", StringType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": ["1", "2"],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("tf_fun_col_0_", StringType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "tf_fun_col_0_": ["11", "21"],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        @udf(str, drop="col_0")
        def tf_fun(col_0):
            return col_0 + "1"

        td = self._create_training_dataset()
        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun,
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_double(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("tf_fun_col_0_", DoubleType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "tf_fun_col_0_": [2.0, 3.0],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        # Arrange
        @udf(float, drop="col_0")
        def tf_fun(col_0):
            return col_0 + 1.0

        td = self._create_training_dataset()
        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun,
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_datetime_no_tz(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", TimestampType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1640995200),
                    datetime.datetime.utcfromtimestamp(1640995201),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )

        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("tf_fun_col_0_", TimestampType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "tf_fun_col_0_": [
                    datetime.datetime.utcfromtimestamp(1640995200)
                    + datetime.timedelta(milliseconds=1),
                    datetime.datetime.utcfromtimestamp(1640995201)
                    + datetime.timedelta(milliseconds=1),
                ],
            }
        )
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        expected_df_localized = expected_df.copy(True)
        expected_df_localized["tf_fun_col_0_"] = expected_df_localized[
            "tf_fun_col_0_"
        ].dt.tz_localize(str(local_tz))
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df_localized, schema=expected_schema
        )

        # Arrange
        @udf(datetime.datetime, drop="col_0")
        def tf_fun(col_0):
            import datetime

            return col_0 + datetime.timedelta(milliseconds=1)

        td = self._create_training_dataset()
        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun,
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        # Assert
        self._validate_on_python_engine(
            td, df, expected_df_localized, transformation_functions
        )
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_datetime_tz_utc(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", TimestampType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1640995200),
                    datetime.datetime.utcfromtimestamp(1640995201),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("tf_fun_col_0_", TimestampType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "tf_fun_col_0_": [
                    datetime.datetime.utcfromtimestamp(1640995200)
                    + datetime.timedelta(milliseconds=1),
                    datetime.datetime.utcfromtimestamp(1640995201)
                    + datetime.timedelta(milliseconds=1),
                ],
            }
        )
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        expected_df_localized = expected_df.copy(True)
        expected_df_localized["tf_fun_col_0_"] = expected_df_localized[
            "tf_fun_col_0_"
        ].dt.tz_localize(str(local_tz))
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df_localized, schema=expected_schema
        )

        # Arrange
        @udf(datetime.datetime, drop="col_0")
        def tf_fun(col_0) -> datetime.datetime:
            import datetime

            return (col_0 + datetime.timedelta(milliseconds=1)).dt.tz_localize(
                datetime.timezone.utc
            )

        td = self._create_training_dataset()
        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun,
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        # Assert
        self._validate_on_python_engine(
            td, df, expected_df_localized, transformation_functions
        )
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_datetime_tz_pst(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", TimestampType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1640995200),
                    datetime.datetime.utcfromtimestamp(1640995201),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("tf_fun_col_0_", TimestampType(), True),
            ]
        )

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "tf_fun_col_0_": [
                    datetime.datetime.utcfromtimestamp(1640995200)
                    + datetime.timedelta(milliseconds=1),
                    datetime.datetime.utcfromtimestamp(1640995201)
                    + datetime.timedelta(milliseconds=1),
                ],
            }
        )
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        expected_df_localized = expected_df.copy(True)
        expected_df_localized["tf_fun_col_0_"] = expected_df_localized[
            "tf_fun_col_0_"
        ].dt.tz_localize(str(local_tz))
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df_localized, schema=expected_schema
        )

        # Arrange
        @udf(datetime.datetime, drop="col_0")
        def tf_fun(col_0) -> datetime.datetime:
            import datetime

            import pytz

            pdt = pytz.timezone("US/Pacific")
            return (col_0 + datetime.timedelta(milliseconds=1)).dt.tz_localize(pdt)

        td = self._create_training_dataset()
        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun,
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        # Assert
        self._validate_on_python_engine(
            td, df, expected_df_localized, transformation_functions
        )
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_datetime_ts_none(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", TimestampType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1640995200),
                    datetime.datetime.utcfromtimestamp(1640995201),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("tf_fun_col_0_", TimestampType(), True),
            ]
        )

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "tf_fun_col_0_": [
                    None,
                    datetime.datetime.utcfromtimestamp(1640995201)
                    + datetime.timedelta(milliseconds=1),
                ],
            }
        )
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        expected_df_localized = expected_df.copy(True)
        expected_df_localized["tf_fun_col_0_"] = expected_df_localized[
            "tf_fun_col_0_"
        ].dt.tz_localize(str(local_tz))
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df_localized, schema=expected_schema
        )

        # Arrange
        @udf(datetime.datetime, drop=["col_0"])
        def tf_fun(col_0) -> datetime.datetime:
            import datetime

            return pd.Series(
                None
                if data == datetime.datetime.utcfromtimestamp(1640995200)
                else data + datetime.timedelta(milliseconds=1)
                for data in col_0
            )

        td = self._create_training_dataset()
        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun,
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        # Assert
        self._validate_on_python_engine(
            td, df, expected_df_localized, transformation_functions
        )
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_date(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        spark_engine = spark.Engine()

        schema = StructType(
            [
                StructField("col_0", DateType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1641045600).date(),
                    datetime.datetime.utcfromtimestamp(1641132000).date(),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
                StructField("tf_fun_col_0_", DateType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "tf_fun_col_0_": [
                    datetime.datetime.utcfromtimestamp(1641045600).date()
                    + datetime.timedelta(days=1),
                    datetime.datetime.utcfromtimestamp(1641132000).date()
                    + datetime.timedelta(days=1),
                ],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        @udf(datetime.date, drop=["col_0"])
        def tf_fun(col_0):
            import datetime

            return col_0 + datetime.timedelta(days=1)

        td = self._create_training_dataset()
        transformation_functions = [
            transformation_function.TransformationFunction(
                hopsworks_udf=tf_fun,
                featurestore_id=99,
                transformation_type=UDFType.MODEL_DEPENDENT,
            )
        ]

        # Assert
        self._validate_on_python_engine(td, df, expected_df, transformation_functions)
        self._validate_on_spark_engine(
            td, spark_df, expected_spark_df, transformation_functions
        )

    def test_apply_plus_one_invalid_type(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        # Arrange
        with pytest.raises(FeatureStoreException) as e_info:

            @udf(list, drop="a")
            def tf_fun(a):
                return a + 1

        assert (
            str(e_info.value)
            == f"Output type {list} is not supported. Please refer to the documentation to get more information on the supported types."
        )
