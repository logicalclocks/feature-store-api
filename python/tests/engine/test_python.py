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
import decimal

import pytest
import pandas as pd
import numpy as np
import pyarrow as pa

from datetime import datetime, date
from hsfs import (
    storage_connector,
    feature_group,
    training_dataset,
    feature_view,
    transformation_function,
    feature,
    util,
)
from hsfs.engine import python
from hsfs.core import inode, execution, job
from hsfs.constructor import query
from hsfs.client import exceptions
from hsfs.constructor.hudi_feature_group_alias import HudiFeatureGroupAlias
from hsfs.training_dataset_feature import TrainingDatasetFeature


class TestPython:
    def test_sql(self, mocker):
        # Arrange
        mock_python_engine_sql_offline = mocker.patch(
            "hsfs.engine.python.Engine._sql_offline"
        )
        mock_python_engine_jdbc = mocker.patch("hsfs.engine.python.Engine._jdbc")

        python_engine = python.Engine()

        # Act
        python_engine.sql(
            sql_query=None,
            feature_store=None,
            online_conn=None,
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_python_engine_sql_offline.call_count == 1
        assert mock_python_engine_jdbc.call_count == 0

    def test_sql_online_conn(self, mocker):
        # Arrange
        mock_python_engine_sql_offline = mocker.patch(
            "hsfs.engine.python.Engine._sql_offline"
        )
        mock_python_engine_jdbc = mocker.patch("hsfs.engine.python.Engine._jdbc")

        python_engine = python.Engine()

        # Act
        python_engine.sql(
            sql_query=None,
            feature_store=None,
            online_conn=mocker.Mock(),
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_python_engine_sql_offline.call_count == 0
        assert mock_python_engine_jdbc.call_count == 1

    def test_sql_offline(self, mocker):
        # Arrange
        mock_python_engine_create_hive_connection = mocker.patch(
            "hsfs.engine.python.Engine._create_hive_connection"
        )
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )

        python_engine = python.Engine()

        # Act
        python_engine._sql_offline(
            sql_query=None, feature_store=None, dataframe_type=None
        )

        # Assert
        assert mock_python_engine_create_hive_connection.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_jdbc(self, mocker):
        # Arrange
        mock_util_create_mysql_engine = mocker.patch("hsfs.util.create_mysql_engine")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )

        python_engine = python.Engine()

        # Act
        python_engine._jdbc(
            sql_query=None, connector=None, dataframe_type=None, read_options={}
        )

        # Assert
        assert mock_util_create_mysql_engine.call_count == 1
        assert mock_client_get_instance.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_jdbc_read_options(self, mocker):
        # Arrange
        mock_util_create_mysql_engine = mocker.patch("hsfs.util.create_mysql_engine")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )

        python_engine = python.Engine()

        # Act
        python_engine._jdbc(
            sql_query=None,
            connector=None,
            dataframe_type=None,
            read_options={"external": ""},
        )

        # Assert
        assert mock_util_create_mysql_engine.call_count == 1
        assert mock_client_get_instance.call_count == 0
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_read_hopsfs_connector(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        python_engine.read(
            storage_connector=connector,
            data_format=None,
            read_options=None,
            location=None,
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert mock_python_engine_read_s3.call_count == 0

    def test_read_s3_connector(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        python_engine.read(
            storage_connector=connector,
            data_format=None,
            read_options=None,
            location=None,
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 0
        assert mock_python_engine_read_s3.call_count == 1

    def test_read_other_connector(self, mocker):
        # Arrange
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.JdbcConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.read(
                storage_connector=connector,
                data_format=None,
                read_options=None,
                location=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "JDBC Storage Connectors for training datasets are not supported yet for external environments."
        )
        assert mock_python_engine_read_hopsfs.call_count == 0
        assert mock_python_engine_read_s3.call_count == 0

    def test_read_pandas_csv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_pandas(data_format="csv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_pandas_tsv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_pandas(data_format="tsv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_pandas_parquet(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        mock_obj = mocker.Mock()
        mock_obj.read.return_value = bytes()

        # Act
        python_engine._read_pandas(data_format="parquet", obj=mock_obj)

        # Assert
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 1

    def test_read_pandas_other(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._read_pandas(data_format="ocr", obj=None)

        # Assert
        assert (
            str(e_info.value)
            == "ocr training dataset format is not supported to read as pandas dataframe."
        )
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_hopsfs(self, mocker):
        # Arrange
        mock_python_engine_read_hopsfs_rest = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_rest"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_rest.call_count == 1

    def test_read_hopsfs_pydoop(self, mocker):
        # Arrange
        mymodule = mocker.Mock()
        mymodule.hdfs.return_value = mocker.Mock()
        mymodule.hdfs.ls.return_value = ["path_1", "path_2"]
        mymodule.hdfs.path.isfile.return_value = False
        mymodule.hdfs.path.getsize.return_value = 0
        mocker.patch.dict("sys.modules", pydoop=mymodule)
        mock_python_engine_read_hopsfs_rest = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_rest"
        )
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mymodule.call_count == 0
        assert mock_python_engine_read_hopsfs_rest.call_count == 0
        assert mock_python_engine_read_pandas.call_count == 0

    def test_read_hopsfs_pydoop_isfile(self, mocker):
        # Arrange
        mymodule = mocker.Mock()
        mymodule.hdfs.return_value = mocker.Mock()
        mymodule.hdfs.ls.return_value = ["path_1", "path_2"]
        mymodule.hdfs.path.isfile.return_value = True
        mymodule.hdfs.path.getsize.return_value = 0
        mocker.patch.dict("sys.modules", pydoop=mymodule)
        mock_python_engine_read_hopsfs_rest = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_rest"
        )
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_rest.call_count == 0
        assert mock_python_engine_read_pandas.call_count == 0

    def test_read_hopsfs_pydoop_getsize(self, mocker):
        # Arrange
        mymodule = mocker.Mock()
        mymodule.hdfs.return_value = mocker.Mock()
        mymodule.hdfs.ls.return_value = ["path_1", "path_2"]
        mymodule.hdfs.path.isfile.return_value = False
        mymodule.hdfs.path.getsize.return_value = 100
        mocker.patch.dict("sys.modules", pydoop=mymodule)
        mock_python_engine_read_hopsfs_rest = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_rest"
        )
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_rest.call_count == 0
        assert mock_python_engine_read_pandas.call_count == 0

    def test_read_hopsfs_pydoop_isfile_getsize(self, mocker):
        # Arrange
        mymodule = mocker.Mock()
        mymodule.hdfs.return_value = mocker.Mock()
        mymodule.hdfs.ls.return_value = ["path_1", "path_2"]
        mymodule.hdfs.path.isfile.return_value = True
        mymodule.hdfs.path.getsize.return_value = 100
        mocker.patch.dict("sys.modules", pydoop=mymodule)
        mock_python_engine_read_hopsfs_rest = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_rest"
        )
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_rest.call_count == 0
        assert mock_python_engine_read_pandas.call_count == 2

    def test_read_hopsfs_rest(self, mocker):
        # Arrange
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        i = inode.Inode(attributes={"path": "test_path"})

        mock_dataset_api.return_value.list_files.return_value = (0, [i, i, i])
        mock_dataset_api.return_value.read_content.return_value.content = bytes()

        # Act
        python_engine._read_hopsfs_rest(location=None, data_format=None)

        # Assert
        assert mock_dataset_api.return_value.list_files.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 3

    def test_read_s3(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        mock_boto3_client.return_value.list_objects_v2.return_value = {
            "is_truncated": False,
            "Contents": [
                {"Key": "test", "Size": 1, "Body": ""},
                {"Key": "test1", "Size": 1, "Body": ""},
            ],
        }

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" not in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 2

    def test_read_s3_session_token(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1, session_token="test_token"
        )

        mock_boto3_client.return_value.list_objects_v2.return_value = {
            "is_truncated": False,
            "Contents": [
                {"Key": "test", "Size": 1, "Body": ""},
                {"Key": "test1", "Size": 1, "Body": ""},
            ],
        }

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 2

    def test_read_s3_next_continuation_token(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        mock_boto3_client.return_value.list_objects_v2.side_effect = [
            {
                "is_truncated": True,
                "NextContinuationToken": "test_token",
                "Contents": [
                    {"Key": "test", "Size": 1, "Body": ""},
                    {"Key": "test1", "Size": 1, "Body": ""},
                ],
            },
            {
                "is_truncated": False,
                "Contents": [
                    {"Key": "test2", "Size": 1, "Body": ""},
                    {"Key": "test3", "Size": 1, "Body": ""},
                ],
            },
        ]

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" not in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 4

    def test_read_options(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.read_options(data_format=None, provided_options=None)

        # Assert
        assert result == {}

    def test_read_options_stream_source(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.read_stream(
                storage_connector=None,
                message_format=None,
                schema=None,
                options=None,
                include_metadata=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Streaming Sources are not supported for pure Python Environments."
        )

    def test_show(self, mocker):
        # Arrange
        mock_python_engine_sql = mocker.patch("hsfs.engine.python.Engine.sql")

        python_engine = python.Engine()

        # Act
        python_engine.show(sql_query=None, feature_store=None, n=None, online_conn=None)

        # Assert
        assert mock_python_engine_sql.call_count == 1

    def test_cast_column_type(self, mocker):
        class LabelIndex:
            def __init__(self, label, index):
                self.label = label
                self.index = index

        python_engine = python.Engine()
        d = {
            "string": ["s"],
            "bigint": ["1"],
            "int": ["1"],
            "smallint": ["1"],
            "tinyint": ["1"],
            "float": ["1"],
            "double": ["1"],
            "timestamp": [1641340800000],
            "boolean": ["False"],
            "date": ["2022-01-27"],
            "binary": ["1"],
            "array<string>": [["123"]],
            "struc": [LabelIndex("0", "1")],
            "decimal": ["1.1"],
        }
        df = pd.DataFrame(data=d)
        schema = [
            TrainingDatasetFeature("string", type="string"),
            TrainingDatasetFeature("bigint", type="bigint"),
            TrainingDatasetFeature("int", type="int"),
            TrainingDatasetFeature("smallint", type="smallint"),
            TrainingDatasetFeature("tinyint", type="tinyint"),
            TrainingDatasetFeature("float", type="float"),
            TrainingDatasetFeature("double", type="double"),
            TrainingDatasetFeature("timestamp", type="timestamp"),
            TrainingDatasetFeature("boolean", type="boolean"),
            TrainingDatasetFeature("date", type="date"),
            TrainingDatasetFeature("binary", type="binary"),
            TrainingDatasetFeature("array<string>", type="array<string>"),
            TrainingDatasetFeature("struc", type="struct<label:string,index:int>"),
            TrainingDatasetFeature("decimal", type="decimal"),
        ]
        cast_df = python_engine.cast_column_type(df, schema)
        expected = {
            "string": object,
            "bigint": np.dtype("int64"),
            "int": np.dtype("int32"),
            "smallint": np.dtype("int16"),
            "tinyint": np.dtype("int8"),
            "float": np.dtype("float32"),
            "double": np.dtype("float64"),
            "timestamp": np.dtype("datetime64[ns]"),
            "boolean": np.dtype("bool"),
            "date": np.dtype(date),
            "binary": np.dtype("S1"),  # pandas converted string to bytes8 == 'S1'
            "array<string>": object,
            "struc": object,
            "decimal": np.dtype(decimal.Decimal),
        }
        for col in cast_df.columns:
            assert cast_df[col].dtype == expected[col]

    def test_register_external_temporary_table(self, mocker):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.register_external_temporary_table(
            external_fg=None, alias=None
        )

        # Assert
        assert result is None

    def test_register_hudi_temporary_table(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.register_hudi_temporary_table(
            hudi_fg_alias=None,
            feature_store_id=None,
            feature_store_name=None,
            read_options=None,
        )

        # Assert
        assert result is None

    def test_register_hudi_temporary_table_time_travel(self):
        # Arrange
        python_engine = python.Engine()
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )
        q = HudiFeatureGroupAlias(
            fg.to_dict(),
            "fg",
            left_feature_group_end_timestamp="20220101",
            left_feature_group_start_timestamp="20220101",
        )
        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.register_hudi_temporary_table(
                hudi_fg_alias=q,
                feature_store_id=None,
                feature_store_name=None,
                read_options=None,
            )

        # Assert
        assert str(e_info.value) == (
            "Hive engine on Python environments does not support incremental queries. "
            + "Read feature group without timestamp to retrieve latest snapshot or switch to "
            + "environment with Spark Engine."
        )

    def test_register_hudi_temporary_table_time_travel_sub_query(self):
        # Arrange
        python_engine = python.Engine()
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )
        q = HudiFeatureGroupAlias(
            fg.to_dict(),
            "fg",
            left_feature_group_end_timestamp="20220101",
            left_feature_group_start_timestamp="20220101",
        )
        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.register_hudi_temporary_table(
                hudi_fg_alias=q,
                feature_store_id=None,
                feature_store_name=None,
                read_options=None,
            )

        # Assert
        assert str(e_info.value) == (
            "Hive engine on Python environments does not support incremental queries. "
            + "Read feature group without timestamp to retrieve latest snapshot or switch to "
            + "environment with Spark Engine."
        )

    def test_profile(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"test_key": "test_value"},
            {"test_key": "test_value"},
        ]

        d = {"col1": ["1", "2"], "col2": ["3", "4"]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result == '{"columns": [{"test_key": "test_value", "dataType": "Integral", '
            '"isDataTypeInferred": "false", "column": "col1", "completeness": 1}, '
            '{"test_key": "test_value", "dataType": "Integral", "isDataTypeInferred": '
            '"false", "column": "col2", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 2

    def test_profile_relevant_columns(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.return_value = {
            "test_key": "test_value"
        }

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=["col1"],
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"test_key": "test_value", "dataType": "Fractional", '
            '"isDataTypeInferred": "false", "column": "col1", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 1

    def test_convert_pandas_statistics(self):
        # Arrange
        python_engine = python.Engine()

        stat = {
            "25%": 25,
            "50%": 50,
            "75%": 75,
            "mean": 50,
            "count": 100,
            "max": 10,
            "std": 33,
            "min": 1,
        }

        # Act
        result = python_engine._convert_pandas_statistics(stat=stat)

        # Assert
        assert result == {
            "approxPercentiles": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                25,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                50,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                75,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            "maximum": 10,
            "mean": 50,
            "minimum": 1,
            "stdDev": 33,
            "sum": 5000,
        }

    def test_validate(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.validate(dataframe=None, expectations=None, log_activity=True)

        # Assert
        assert (
            str(e_info.value)
            == "Deequ data validation is only available with Spark Engine. Use validate_with_great_expectations"
        )

    def test_validate_with_great_expectations(self, mocker):
        # Arrange
        mock_ge_from_pandas = mocker.patch("great_expectations.from_pandas")

        python_engine = python.Engine()

        # Act
        python_engine.validate_with_great_expectations(
            dataframe=None, expectation_suite=None, ge_validate_kwargs={}
        )

        # Assert
        assert mock_ge_from_pandas.call_count == 1

    def test_set_job_group(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.set_job_group(group_id=None, description=None)

        # Assert
        assert result is None

    def test_convert_to_default_dataframe(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains upper case letters in feature names: `['Col1']`. Feature names are sanitized to lower case in the feature store."
        )

    def test_parse_schema_feature_group(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._convert_pandas_type")

        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.parse_schema_feature_group(
            dataframe=df, time_travel_format=None
        )

        # Assert
        assert len(result) == 2
        assert result[0].name == "col1"
        assert result[1].name == "col2"

    def test_parse_schema_training_dataset(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.parse_schema_training_dataset(dataframe=None)

        # Assert
        assert (
            str(e_info.value)
            == "Training dataset creation from Dataframes is not supported in Python environment. Use HSFS Query object instead."
        )

    def test_convert_pandas_type(self, mocker):
        # Arrange
        mock_python_engine_infer_type_pyarrow = mocker.patch(
            "hsfs.engine.python.Engine._infer_type_pyarrow"
        )
        mock_python_engine_convert_simple_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_simple_pandas_type"
        )

        python_engine = python.Engine()

        # Act
        python_engine._convert_pandas_type(dtype=None, arrow_type=None)

        # Assert
        assert mock_python_engine_infer_type_pyarrow.call_count == 0
        assert mock_python_engine_convert_simple_pandas_type.call_count == 1

    def test_convert_pandas_type_object(self, mocker):
        # Arrange
        mock_python_engine_infer_type_pyarrow = mocker.patch(
            "hsfs.engine.python.Engine._infer_type_pyarrow"
        )
        mock_python_engine_convert_simple_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_simple_pandas_type"
        )

        python_engine = python.Engine()

        # Act
        python_engine._convert_pandas_type(dtype=np.dtype("O"), arrow_type=None)

        # Assert
        assert mock_python_engine_infer_type_pyarrow.call_count == 1
        assert mock_python_engine_convert_simple_pandas_type.call_count == 0

    def test_convert_simple_pandas_type_uint8(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("uint8"))

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_uint16(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("uint16"))

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int8(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("int8"))

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int16(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("int16"))

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int32(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("int32"))

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_uint32(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("uint32"))

        # Assert
        assert result == "bigint"

    def test_convert_simple_pandas_type_int64(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("int64"))

        # Assert
        assert result == "bigint"

    def test_convert_simple_pandas_type_float16(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("float16"))

        # Assert
        assert result == "float"

    def test_convert_simple_pandas_type_float32(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("float32"))

        # Assert
        assert result == "float"

    def test_convert_simple_pandas_type_float64(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("float64"))

        # Assert
        assert result == "double"

    def test_convert_simple_pandas_type_datetime64(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(
            dtype=np.dtype("datetime64[ns]")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_bool(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype=np.dtype("bool"))

        # Assert
        assert result == "boolean"

    def test_convert_simple_pandas_type_category(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_type(dtype="category")

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_other(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(ValueError) as e_info:
            python_engine._convert_simple_pandas_type(dtype="other")

        # Assert
        assert str(e_info.value) == "dtype 'other' not supported"

    def test_infer_type_pyarrow_list(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_type"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_type.return_value = "test"

        # Act
        result = python_engine._infer_type_pyarrow(arrow_type=pa.list_(pa.int8()))

        # Assert
        assert result == "array<test>"

    def test_infer_type_pyarrow_struct(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_type"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_type.return_value = "test"

        # Act
        result = python_engine._infer_type_pyarrow(arrow_type=pa.struct({}))

        # Assert
        assert result == "struct<>"

    def test_infer_type_pyarrow_date(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_type"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_type.return_value = "test"

        # Act
        result = python_engine._infer_type_pyarrow(arrow_type=pa.date32())

        # Assert
        assert result == "date"

    def test_infer_type_pyarrow_binary(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_type"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_type.return_value = "test"

        # Act
        result = python_engine._infer_type_pyarrow(arrow_type=pa.binary())

        # Assert
        assert result == "binary"

    def test_infer_type_pyarrow_string(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_type"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_type.return_value = "test"

        # Act
        result = python_engine._infer_type_pyarrow(arrow_type=pa.string())

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_utf8(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_type"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_type.return_value = "test"

        # Act
        result = python_engine._infer_type_pyarrow(arrow_type=pa.utf8())

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_other(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_type = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_type"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_type.return_value = "test"

        # Act
        with pytest.raises(ValueError) as e_info:
            python_engine._infer_type_pyarrow(arrow_type=pa.bool_())

        # Assert
        assert str(e_info.value) == "dtype 'O' (arrow_type 'bool') not supported"

    def test_save_dataframe(self, mocker):
        # Arrange
        mock_python_engine_write_dataframe_kafka = mocker.patch(
            "hsfs.engine.python.Engine._write_dataframe_kafka"
        )
        mock_python_engine_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.legacy_save_dataframe"
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )

        # Act
        python_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_python_engine_write_dataframe_kafka.call_count == 0
        assert mock_python_engine_legacy_save_dataframe.call_count == 1

    def test_save_dataframe_stream(self, mocker):
        # Arrange
        mock_python_engine_write_dataframe_kafka = mocker.patch(
            "hsfs.engine.python.Engine._write_dataframe_kafka"
        )
        mock_python_engine_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.legacy_save_dataframe"
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=True,
        )

        # Act
        python_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_python_engine_write_dataframe_kafka.call_count == 1
        assert mock_python_engine_legacy_save_dataframe.call_count == 0

    def test_legacy_save_dataframe(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._get_app_options")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")
        mocker.patch("hsfs.engine.python.Engine.get_job_url")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        python_engine = python.Engine()

        mock_fg_api.return_value.ingestion.return_value.job = job.Job(
            1, "test_job", None, None, None, None
        )

        # Act
        python_engine.legacy_save_dataframe(
            feature_group=mocker.Mock(),
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options={},
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_fg_api.return_value.ingestion.call_count == 1
        assert mock_dataset_api.return_value.upload.call_count == 1
        assert mock_job_api.return_value.launch.call_count == 1
        assert mock_engine_get_instance.return_value.get_job_url.call_count == 1

    def test_get_training_data(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_python_engine_prepare_transform_split_df = mocker.patch(
            "hsfs.engine.python.Engine._prepare_transform_split_df"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )

        python_engine = python.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.get_training_data(
            training_dataset_obj=td,
            feature_view_obj=None,
            query_obj=mocker.Mock(),
            read_options=None,
        )

        # Assert
        assert mock_python_engine_prepare_transform_split_df.call_count == 0

    def test_get_training_data_splits(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_python_engine_prepare_transform_split_df = mocker.patch(
            "hsfs.engine.python.Engine._prepare_transform_split_df"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )

        python_engine = python.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"name": "value"},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.get_training_data(
            training_dataset_obj=td,
            feature_view_obj=None,
            query_obj=mocker.Mock(),
            read_options=None,
        )

        # Assert
        assert mock_python_engine_prepare_transform_split_df.call_count == 1

    def test_split_labels(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(df=df, labels=None)

        # Assert
        assert str(result_df) == "   Col1  col2\n0     1     3\n1     2     4"
        assert str(result_df_split) == "None"

    def test_split_labels_labels(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(df=df, labels="col1")

        # Assert
        assert str(result_df) == "   col2\n0     3\n1     4"
        assert str(result_df_split) == "0    1\n1    2\nName: col1, dtype: int64"

    def test_prepare_transform_split_df_random_split(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_random_split = mocker.patch(
            "hsfs.engine.python.Engine._random_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        q = query.Query(left_feature_group=None, left_features=None)

        mock_python_engine_random_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=None,
            read_option=None,
        )

        # Assert
        assert mock_python_engine_random_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_prepare_transform_split_df_time_split_td_features(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_time_series_split = mocker.patch(
            "hsfs.engine.python.Engine._time_series_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(name="event_time", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
            features=[f, f1, f2],
        )

        q = query.Query(left_feature_group=fg, left_features=[])

        mock_python_engine_time_series_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=None,
            read_option=None,
        )

        # Assert
        assert mock_python_engine_time_series_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_prepare_transform_split_df_time_split_query_features(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_time_series_split = mocker.patch(
            "hsfs.engine.python.Engine._time_series_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        mock_python_engine_time_series_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(name="event_time", type="str")

        q = query.Query(left_feature_group=fg, left_features=[f, f1, f2])

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=None,
            read_option=None,
        )

        # Assert
        assert mock_python_engine_time_series_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_random_split(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["test_split1", "test_split2"]
        for column in list(result):
            assert not result[column].empty

    def test_random_split_bad_percentage(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.2},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert (
            str(e_info.value)
            == "Sum of split ratios should be 1 and each values should be in range (0, 1)"
        )

    def test_time_series_split(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [], "col2": [], "event_time": []}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_time_series_split_drop_event_time(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [], "col2": [], "event_time": []}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}
        expected["train"] = expected["train"].drop(["event_time"], axis=1)
        expected["test"] = expected["test"].drop(["event_time"], axis=1)

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=True,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_time_series_split_event_time(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_convert_to_unix_timestamp_pandas(self):
        # Act
        result = util.convert_event_time_to_timestamp(
            event_time=pd.Timestamp("2017-01-01")
        )

        # Assert
        assert result == 1483228800000.0

    def test_convert_to_unix_timestamp_str(self, mocker):
        # Arrange
        mock_util_get_timestamp_from_date_string = mocker.patch(
            "hsfs.util.get_timestamp_from_date_string"
        )

        mock_util_get_timestamp_from_date_string.return_value = 1483225200000

        # Act
        result = util.convert_event_time_to_timestamp(
            event_time="2017-01-01 00-00-00-000"
        )

        # Assert
        assert result == 1483225200000

    def test_convert_to_unix_timestamp_int(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=1483225200)

        # Assert
        assert result == 1483225200000

    def test_convert_to_unix_timestamp_datetime(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=datetime(2022, 9, 18))

        # Assert
        assert result == 1663459200000

    def test_convert_to_unix_timestamp_date(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=date(2022, 9, 18))

        # Assert
        assert result == 1663459200000

    def test_convert_to_unix_timestamp_pandas_datetime(self):
        # Act
        result = util.convert_event_time_to_timestamp(
            event_time=pd.Timestamp("2022-09-18")
        )

        # Assert
        assert result == 1663459200000

    def test_write_training_dataset(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mocker.patch("hsfs.engine.python.Engine.get_job_url")
        mock_python_engine_wait_for_job = mocker.patch(
            "hsfs.engine.python.Engine.wait_for_job"
        )

        python_engine = python.Engine()

        # Act
        with pytest.raises(Exception) as e_info:
            python_engine.write_training_dataset(
                training_dataset=None,
                dataset=None,
                user_write_options={},
                save_mode=None,
                feature_view_obj=None,
                to_df=False,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Currently only query based training datasets are supported by the Python engine"
        )
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 0
        assert mock_td_api.return_value.compute.call_count == 0
        assert mock_python_engine_wait_for_job.call_count == 0

    def test_write_training_dataset_query_td(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mocker.patch("hsfs.engine.python.Engine.get_job_url")
        mock_python_engine_wait_for_job = mocker.patch(
            "hsfs.engine.python.Engine.wait_for_job"
        )

        python_engine = python.Engine()

        q = query.Query(None, None)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.write_training_dataset(
            training_dataset=td,
            dataset=q,
            user_write_options={},
            save_mode=None,
            feature_view_obj=None,
            to_df=False,
        )

        # Assert
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 0
        assert mock_td_api.return_value.compute.call_count == 1
        assert mock_python_engine_wait_for_job.call_count == 1

    def test_write_training_dataset_query_fv(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mocker.patch("hsfs.engine.python.Engine.get_job_url")
        mock_python_engine_wait_for_job = mocker.patch(
            "hsfs.engine.python.Engine.wait_for_job"
        )

        python_engine = python.Engine()

        q = query.Query(None, None)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=q,
            featurestore_id=99,
            labels=[],
        )

        # Act
        python_engine.write_training_dataset(
            training_dataset=td,
            dataset=q,
            user_write_options={},
            save_mode=None,
            feature_view_obj=fv,
            to_df=False,
        )

        # Assert
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 1
        assert mock_td_api.return_value.compute.call_count == 0
        assert mock_python_engine_wait_for_job.call_count == 1

    def test_create_hive_connection(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_pyhive_conn = mocker.patch("pyhive.hive.Connection")

        python_engine = python.Engine()

        # Act
        python_engine._create_hive_connection(feature_store=None)

        # Assert
        assert mock_pyhive_conn.call_count == 1

    def test_return_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="default"
        )

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"

    def test_return_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="pandas"
        )

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"

    def test_return_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="numpy"
        )

        # Assert
        assert str(result) == "[[1 3]\n [2 4]]"

    def test_return_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="python"
        )

        # Assert
        assert result == [[1, 3], [2, 4]]

    def test_return_dataframe_type_other(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._return_dataframe_type(dataframe=df, dataframe_type="other")

        # Assert
        assert (
            str(e_info.value)
            == "Dataframe type `other` not supported on this platform."
        )

    def test_is_spark_dataframe(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.is_spark_dataframe(dataframe=None)

        # Assert
        assert result is False

    def test_save_stream_dataframe(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.save_stream_dataframe(
                feature_group=None,
                dataframe=None,
                query_name=None,
                output_mode=None,
                await_termination=None,
                timeout=None,
                write_options=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Stream ingestion is not available on Python environments, because it requires Spark as engine."
        )

    def test_save_empty_dataframe(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.save_empty_dataframe(feature_group=None)

        # Assert
        assert result is None

    def test_get_job_url(self, mocker):
        # Arrange
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        # Act
        python_engine.get_job_url(href="1/2/3/4/5/6/7/8")

        # Assert
        assert (
            mock_client_get_instance.return_value.replace_public_host.call_args[0][
                0
            ].path
            == "p/5/jobs/named/7/executions"
        )

    def test_get_app_options(self, mocker):
        # Arrange
        mock_ingestion_job_conf = mocker.patch(
            "hsfs.core.ingestion_job_conf.IngestionJobConf"
        )

        python_engine = python.Engine()

        # Act
        python_engine._get_app_options(user_write_options={"spark": 1, "test": 2})

        # Assert
        assert mock_ingestion_job_conf.call_count == 1
        assert mock_ingestion_job_conf.call_args[1]["write_options"] == {"test": 2}

    def test_wait_for_job(self, mocker):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        python_engine = python.Engine()

        # Act
        python_engine.wait_for_job(job=None)

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1

    def test_wait_for_job_wait_for_job_true(self, mocker):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        python_engine = python.Engine()

        # Act
        python_engine.wait_for_job(job=None, await_termination=True)

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1

    def test_wait_for_job_wait_for_job_false(self, mocker):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        python_engine = python.Engine()

        # Act
        python_engine.wait_for_job(job=None, await_termination=False)

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 0

    def test_wait_for_job_final_status_succeeded(self, mocker):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        python_engine = python.Engine()

        mock_job_api.return_value.last_execution.return_value = [
            execution.Execution(id=1, state=None, final_status="succeeded")
        ]

        # Act
        python_engine.wait_for_job(job=None)

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1

    def test_wait_for_job_final_status_failed(self, mocker):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        python_engine = python.Engine()

        mock_job_api.return_value.last_execution.return_value = [
            execution.Execution(id=1, state=None, final_status="failed")
        ]

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.wait_for_job(job=None)

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1
        assert (
            str(e_info.value)
            == "The Hopsworks Job failed, use the Hopsworks UI to access the job logs"
        )

    def test_wait_for_job_final_status_killed(self, mocker):
        # Arrange
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")

        python_engine = python.Engine()

        mock_job_api.return_value.last_execution.return_value = [
            execution.Execution(id=1, state=None, final_status="killed")
        ]

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.wait_for_job(job=None)

        # Assert
        assert mock_job_api.return_value.last_execution.call_count == 1
        assert str(e_info.value) == "The Hopsworks Job was stopped"

    def test_add_file(self):
        # Arrange
        python_engine = python.Engine()

        file = None

        # Act
        result = python_engine.add_file(file=file)

        # Assert
        assert result == file

    def test_apply_transformation_function(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        def plus_one(a):
            return a + 1

        tf = transformation_function.TransformationFunction(
            99,
            transformation_fn=plus_one,
            builtin_source_code="",
            output_type="int",
        )

        transformation_fn_dict = dict()

        transformation_fn_dict["tf_name"] = tf

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            id=10,
            transformation_functions=transformation_fn_dict,
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=td.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["tf_name"]) == 2
        assert result["tf_name"][0] == 2
        assert result["tf_name"][1] == 3

    def test_get_unique_values(self):
        # Arrange
        python_engine = python.Engine()

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        result = python_engine.get_unique_values(
            feature_dataframe=df, feature_name="col1"
        )

        # Assert
        assert len(result) == 3
        assert 1 in result
        assert 2 in result
        assert 3 in result

    def test_write_dataframe_kafka(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.engine.python.Engine._get_encoder_func")
        mocker.patch("hsfs.engine.python.Engine._encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.engine.python.Engine._kafka_produce"
        )
        mock_job_api = mocker.patch("hsfs.core.job_api.JobApi")
        mocker.patch("hsfs.engine.python.Engine.get_job_url")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")

        mock_job_api.return_value.get.return_value = job.Job(
            1, "test_job", None, None, None, None
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_backfill": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        assert mock_engine_get_instance.return_value.wait_for_job.call_count == 1

    def test_kafka_produce(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        producer = mocker.Mock()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )

        # Act
        python_engine._kafka_produce(
            producer=producer,
            feature_group=fg,
            key=None,
            encoded_row=None,
            acked=None,
            offline_write_options={},
        )

        # Assert
        assert producer.produce.call_count == 1
        assert producer.poll.call_count == 1

    def test_kafka_produce_buffer_error(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_print = mocker.patch("builtins.print")

        python_engine = python.Engine()

        producer = mocker.Mock()
        producer.produce.side_effect = [BufferError("test_error"), None]

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )

        # Act
        python_engine._kafka_produce(
            producer=producer,
            feature_group=fg,
            key=None,
            encoded_row=None,
            acked=None,
            offline_write_options={"debug_kafka": True},
        )

        # Assert
        assert producer.produce.call_count == 2
        assert producer.poll.call_count == 2
        assert mock_print.call_count == 1
        assert mock_print.call_args[0][0] == "Caught: test_error"

    def test_encode_complex_features(self):
        # Arrange
        python_engine = python.Engine()

        def test_utf(value, bytes_io):
            bytes_io.write(bytes(value, "utf-8"))

        # Act
        result = python_engine._encode_complex_features(
            feature_writers={"one": test_utf, "two": test_utf},
            row={"one": "1", "two": "2"},
        )

        # Assert
        assert len(result) == 2
        assert result == {"one": b"1", "two": b"2"}

    def test_get_encoder_func(self, mocker):
        # Arrange
        mock_json_loads = mocker.patch("json.loads")
        mock_avro_schema_parse = mocker.patch("avro.schema.parse")

        python_engine = python.Engine()
        python.HAS_FAST = False

        # Act
        result = python_engine._get_encoder_func(
            writer_schema='{"type" : "record",'
            '"namespace" : "Tutorialspoint",'
            '"name" : "Employee",'
            '"fields" : [{ "name" : "Name" , "type" : "string" },'
            '{ "name" : "Age" , "type" : "int" }]}'
        )

        # Assert
        assert result is not None
        assert mock_json_loads.call_count == 0
        assert mock_avro_schema_parse.call_count == 1

    def test_get_encoder_func_fast(self, mocker):
        # Arrange
        mock_json_loads = mocker.patch(
            "json.loads",
            return_value={
                "type": "record",
                "namespace": "Tutorialspoint",
                "name": "Employee",
                "fields": [
                    {"name": "Name", "type": "string"},
                    {"name": "Age", "type": "int"},
                ],
            },
        )
        mock_avro_schema_parse = mocker.patch("avro.schema.parse")

        python_engine = python.Engine()
        python.HAS_FAST = True

        # Act
        result = python_engine._get_encoder_func(
            writer_schema='{"type" : "record",'
            '"namespace" : "Tutorialspoint",'
            '"name" : "Employee",'
            '"fields" : [{ "name" : "Name" , "type" : "string" },'
            '{ "name" : "Age" , "type" : "int" }]}'
        )

        # Assert
        assert result is not None
        assert mock_json_loads.call_count == 1
        assert mock_avro_schema_parse.call_count == 0

    def test_get_kafka_config(self, mocker):
        # Arrange
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_client_get_instance.return_value._get_ca_chain_path.return_value = (
            "_get_ca_chain_path"
        )
        mock_client_get_instance.return_value._get_client_cert_path.return_value = (
            "_get_client_cert_path"
        )
        mock_client_get_instance.return_value._get_client_key_path.return_value = (
            "_get_client_key_path"
        )
        mocker.patch("socket.gethostname", return_value="gethostname")
        mock_kafka_api = mocker.patch("hsfs.core.kafka_api.KafkaApi")

        python_engine = python.Engine()

        mock_kafka_api.return_value.get_broker_endpoints.return_value = [
            "INTERNAL://1",
            "2",
        ]

        # Act
        result = python_engine._get_kafka_config(
            write_options={
                "kafka_producer_config": {"test_name_1": "test_value_1"},
                "internal_kafka": True,
            }
        )

        # Assert
        assert result == {
            "bootstrap.servers": "1,2",
            "client.id": "gethostname",
            "security.protocol": "SSL",
            "ssl.ca.location": "_get_ca_chain_path",
            "ssl.certificate.location": "_get_client_cert_path",
            "ssl.key.location": "_get_client_key_path",
            "test_name_1": "test_value_1",
        }

    def test_get_kafka_config_external(self, mocker):
        # Arrange
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_client_get_instance.return_value._get_ca_chain_path.return_value = (
            "_get_ca_chain_path"
        )
        mock_client_get_instance.return_value._get_client_cert_path.return_value = (
            "_get_client_cert_path"
        )
        mock_client_get_instance.return_value._get_client_key_path.return_value = (
            "_get_client_key_path"
        )
        mocker.patch("socket.gethostname", return_value="gethostname")
        mock_kafka_api = mocker.patch("hsfs.core.kafka_api.KafkaApi")

        python_engine = python.Engine()

        mock_kafka_api.return_value.get_broker_endpoints.return_value = [
            "EXTERNAL://1",
            "2",
        ]

        # Act
        result = python_engine._get_kafka_config(
            write_options={"kafka_producer_config": {"test_name_1": "test_value_1"}}
        )

        # Assert
        assert result == {
            "bootstrap.servers": "1,2",
            "client.id": "gethostname",
            "security.protocol": "SSL",
            "ssl.ca.location": "_get_ca_chain_path",
            "ssl.certificate.location": "_get_client_cert_path",
            "ssl.key.location": "_get_client_key_path",
            "test_name_1": "test_value_1",
        }
