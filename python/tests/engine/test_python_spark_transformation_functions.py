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

import statistics

import pandas as pd
import numpy as np
import datetime
import pytz
import tzlocal

import pytest
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    DateType,
    StringType,
    TimestampType,
    StructType,
    BooleanType,
    StructField,
)

from hsfs import (
    training_dataset,
    transformation_function,
    training_dataset_feature,
)
from hsfs.engine import spark
from hsfs.engine import python
from hsfs.core.transformation_function_engine import TransformationFunctionEngine


class TestPythonSparkTransformationFunctions:
    def _create_training_dataset(
        self, tf_fun, output_type=None, name=None, col="col_0"
    ):
        if isinstance(tf_fun, str):
            tf = transformation_function.TransformationFunction(
                name=name,
                featurestore_id=99,
                transformation_fn=None,
                source_code_content=tf_fun,
                output_type=output_type,
            )
        else:
            tf = transformation_function.TransformationFunction(
                featurestore_id=99,
                transformation_fn=tf_fun,
                builtin_source_code=None,
                output_type=output_type,
            )
        transformation_fn_dict = dict()
        transformation_fn_dict[col] = tf

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
            transformation_functions=transformation_fn_dict,
        )

        return td

    def _validate_on_python_engine(self, td, df, expected_df):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=td.transformation_functions,
            dataset=df,
        )

        assert sorted(result.columns) == sorted(expected_df.columns)
        assert sorted(result.dtypes) == sorted(expected_df.dtypes)
        assert result.equals(expected_df)

    def _validate_on_spark_engine(self, td, spark_df, expected_spark_df):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=td.transformation_functions,
            dataset=spark_df,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_builtin_minmax(self, mocker):
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
                StructField("col_0", DoubleType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [0.5, 1.0],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        tf_fun = (
            '{"module_imports": "from datetime import datetime", "transformer_code": '
            '"def min_max_scaler(value, min_value,max_value):\\n    if value is None:\\n        '
            "return None\\n    else:\\n        try:\\n            return (value - min_value) / (max_value - min_value)\\n"
            '        except ZeroDivisionError:\\n            return 0\\n"}'
        )

        td = self._create_training_dataset(tf_fun, "DOUBLE", "min_max_scaler")

        td.transformation_functions[
            "col_0"
        ] = TransformationFunctionEngine.populate_builtin_fn_arguments(
            "col_0",
            td.transformation_functions["col_0"],
            {
                "columns": [
                    {
                        "column": "col_0",
                        "dataType": "Integral",
                        "minimum": 0,
                        "maximum": 2,
                    }
                ]
            },
        )

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_builtin_labelencoder(self, mocker):
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
                StructField("col_0", IntegerType(), True),
                StructField("col_1", IntegerType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": [0, 1],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )
        expected_df["col_1"] = expected_df["col_1"].astype(np.dtype("int32"))

        # Arrange
        tf_fun = (
            '{"module_imports": "", "transformer_code": "# label encoder\\n'
            "def label_encoder(value, value_to_index):\\n"
            "    # define a mapping of values to integers\\n"
            '    return value_to_index[value]"}'
        )

        td = self._create_training_dataset(tf_fun, "INT", "label_encoder", "col_1")

        td.transformation_functions[
            "col_1"
        ] = TransformationFunctionEngine.populate_builtin_fn_arguments(
            "col_1",
            td.transformation_functions["col_1"],
            {"columns": [{"column": "col_1", "unique_values": ["test_1", "test_2"]}]},
        )

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_builtin_standard_scaler(self, mocker):
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
                StructField("col_0", DoubleType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [-1.0, 1.0],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        tf_fun = (
            '{"module_imports": "from datetime import datetime", "transformer_code": "'
            "def standard_scaler(value, mean, std_dev):\\n    if value is None:\\n        return None\\n    "
            "else:\\n        try:\\n            return (value - mean) / std_dev\\n        except "
            'ZeroDivisionError:\\n            return 0\\n"}'
        )

        td = self._create_training_dataset(tf_fun, "DOUBLE", "standard_scaler")

        mean = statistics.mean([1, 2])
        stddev = statistics.pstdev([1, 2])
        td.transformation_functions[
            "col_0"
        ] = TransformationFunctionEngine.populate_builtin_fn_arguments(
            "col_0",
            td.transformation_functions["col_0"],
            {
                "columns": [
                    {
                        "column": "col_0",
                        "dataType": "Integral",
                        "mean": mean,
                        "stdDev": stddev,
                    }
                ]
            },
        )

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_builtin_robustscaler(self, mocker):
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
                StructField("col_0", DoubleType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [-1.0, 0.0],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        tf_fun = (
            '{"module_imports": "from datetime import datetime", "transformer_code": "'
            "def robust_scaler(value, p25, p50, p75):\\n    if value is None:\\n        "
            "return None\\n    else:\\n        try:\\n            return (value - p50) / (p75 - p25)\\n        "
            'except ZeroDivisionError:\\n            return 0\\n"}\n'
        )

        td = self._create_training_dataset(tf_fun, "DOUBLE", "robust_scaler")

        percentiles = [1] * 100
        percentiles[24] = 1
        percentiles[49] = 2
        percentiles[74] = 2
        td.transformation_functions[
            "col_0"
        ] = TransformationFunctionEngine.populate_builtin_fn_arguments(
            "col_0",
            td.transformation_functions["col_0"],
            {
                "columns": [
                    {
                        "column": "col_0",
                        "dataType": "Integral",
                        "approxPercentiles": percentiles,
                    }
                ]
            },
        )

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

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
                StructField("col_0", IntegerType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [2, 3],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )
        expected_df["col_0"] = expected_df["col_0"].astype(np.dtype("int32"))

        # Arrange
        def tf_fun(a) -> int:
            return a + 1

        td = self._create_training_dataset(tf_fun, "int")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_str(self, mocker):
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
                StructField("col_0", StringType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": ["2", "3"],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        def tf_fun(a) -> int:
            return a + 1

        td = self._create_training_dataset(tf_fun, "string")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

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
                StructField("col_0", DoubleType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [2.0, 3.0],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        # Arrange
        def tf_fun(a) -> np.float64:
            return a + 1.0

        td = self._create_training_dataset(tf_fun, "double")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_datetime_no_tz(self, mocker):
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
                "col_0": [1640995200, 1640995201],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )

        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_0", TimestampType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1640995201),
                    datetime.datetime.utcfromtimestamp(1640995202),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        expected_df_localized = expected_df.copy(True)
        expected_df_localized["col_0"] = expected_df_localized["col_0"].dt.tz_localize(
            str(local_tz)
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df_localized, schema=expected_schema
        )

        # Arrange
        def tf_fun(a) -> datetime.datetime:
            return datetime.datetime.utcfromtimestamp(a + 1)

        td = self._create_training_dataset(tf_fun, "datetime")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_datetime_tz_utc(self, mocker):
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
                "col_0": [1640995200, 1640995201],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_0", TimestampType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1640995201),
                    datetime.datetime.utcfromtimestamp(1640995202),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        expected_df_localized = expected_df.copy(True)
        expected_df_localized["col_0"] = expected_df_localized["col_0"].dt.tz_localize(
            str(local_tz)
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df_localized, schema=expected_schema
        )

        # Arrange
        def tf_fun(a) -> datetime.datetime:
            return datetime.datetime.utcfromtimestamp(a + 1).replace(
                tzinfo=datetime.timezone.utc
            )

        td = self._create_training_dataset(tf_fun, "datetime")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_datetime_tz_pst(self, mocker):
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
                "col_0": [1640995200, 1640995201],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_0", TimestampType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )

        expected_df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1641024001),
                    datetime.datetime.utcfromtimestamp(1641024002),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        expected_df_localized = expected_df.copy(True)
        expected_df_localized["col_0"] = expected_df_localized["col_0"].dt.tz_localize(
            str(local_tz)
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df_localized, schema=expected_schema
        )

        # Arrange
        def tf_fun(a) -> datetime.datetime:
            pdt = pytz.timezone("US/Pacific")
            return pdt.localize(datetime.datetime.utcfromtimestamp(a + 1))

        td = self._create_training_dataset(tf_fun, "datetime")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_datetime_ts_none(self, mocker):
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
                "col_0": [1640995200, 1640995201],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_0", TimestampType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )

        expected_df = pd.DataFrame(
            data={
                "col_0": [
                    None,
                    datetime.datetime.utcfromtimestamp(1640995202),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        expected_df_localized = expected_df.copy(True)
        expected_df_localized["col_0"] = expected_df_localized["col_0"].dt.tz_localize(
            str(local_tz)
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df_localized, schema=expected_schema
        )

        # Arrange
        def tf_fun(a) -> datetime.datetime:
            return (
                None if a == 1640995200 else datetime.datetime.utcfromtimestamp(a + 1)
            )

        td = self._create_training_dataset(tf_fun, "datetime")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_date(self, mocker):
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
                "col_0": [1641045600, 1641132000],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        spark_df = spark_engine._spark_session.createDataFrame(df, schema=schema)

        expected_schema = StructType(
            [
                StructField("col_0", DateType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": [
                    datetime.datetime.utcfromtimestamp(1641045601).date(),
                    datetime.datetime.utcfromtimestamp(1641132001).date(),
                ],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        def tf_fun(a) -> datetime.datetime:
            return datetime.datetime.utcfromtimestamp(a + 1)

        td = self._create_training_dataset(tf_fun, "date")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_no_type(self, mocker):
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
                StructField("col_0", StringType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": ["2", "3"],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        def tf_fun(a) -> int:
            return a + 1

        td = self._create_training_dataset(tf_fun)

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_empty_type(self, mocker):
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
                StructField("col_0", StringType(), True),
                StructField("col_1", StringType(), True),
                StructField("col_2", BooleanType(), True),
            ]
        )
        expected_df = pd.DataFrame(
            data={
                "col_0": ["2", "3"],
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
            }
        )
        expected_spark_df = spark_engine._spark_session.createDataFrame(
            expected_df, schema=expected_schema
        )

        # Arrange
        def tf_fun(a) -> int:
            return a + 1

        td = self._create_training_dataset(tf_fun, "")

        # Assert
        self._validate_on_python_engine(td, df, expected_df)
        self._validate_on_spark_engine(td, spark_df, expected_spark_df)

    def test_apply_plus_one_date_not_supported_type(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        # Arrange
        def tf_fun(a) -> int:
            return a + 1

        # Act
        with pytest.raises(TypeError) as e_info:
            self._create_training_dataset(tf_fun, list)

        # Assert
        assert str(e_info.value) == "Not supported type <class 'list'>."
