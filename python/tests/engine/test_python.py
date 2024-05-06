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
from datetime import date, datetime

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from confluent_kafka.admin import PartitionMetadata, TopicMetadata
from hsfs import (
    engine,
    feature,
    feature_group,
    feature_view,
    storage_connector,
    training_dataset,
    util,
)
from hsfs.client import exceptions
from hsfs.constructor import query
from hsfs.constructor.hudi_feature_group_alias import HudiFeatureGroupAlias
from hsfs.core import inode, job
from hsfs.engine import python
from hsfs.hopsworks_udf import hopsworks_udf
from hsfs.training_dataset_feature import TrainingDatasetFeature
from polars.testing import assert_frame_equal as polars_assert_frame_equal


engine._engine_type = "python"


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
            sql_query="", feature_store=None, dataframe_type="default"
        )

        # Assert
        assert mock_python_engine_create_hive_connection.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_sql_offline_dataframe_type_none(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._create_hive_connection")

        python_engine = python.Engine()

        with pytest.raises(exceptions.FeatureStoreException) as fstore_except:
            # Act
            python_engine._sql_offline(
                sql_query="", feature_store=None, dataframe_type=None
            )
        assert (
            str(fstore_except.value)
            == 'dataframe_type : None not supported. Possible values are "default", "pandas", "polars", "numpy" or "python"'
        )

    def test_jdbc(self, mocker):
        # Arrange
        mock_util_create_mysql_engine = mocker.patch("hsfs.util.create_mysql_engine")
        mocker.patch("hsfs.client.get_instance")
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        python_engine._jdbc(
            sql_query=query, connector=None, dataframe_type="default", read_options={}
        )

        # Assert
        assert mock_util_create_mysql_engine.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_jdbc_dataframe_type_none(self, mocker):
        # Arrange
        mocker.patch("hsfs.util.create_mysql_engine")
        mocker.patch("hsfs.client.get_instance")
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as fstore_except:
            python_engine._jdbc(
                sql_query=query, connector=None, dataframe_type=None, read_options={}
            )

        # Assert
        assert (
            str(fstore_except.value)
            == 'dataframe_type : None not supported. Possible values are "default", "pandas", "polars", "numpy" or "python"'
        )

    def test_jdbc_read_options(self, mocker):
        # Arrange
        mock_util_create_mysql_engine = mocker.patch("hsfs.util.create_mysql_engine")
        mocker.patch("hsfs.client.get_instance")
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        python_engine._jdbc(
            sql_query=query,
            connector=None,
            dataframe_type="default",
            read_options={"external": ""},
        )

        # Assert
        assert mock_util_create_mysql_engine.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_read_none_data_format(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.read(
                storage_connector=None,
                data_format=None,
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

    def test_read_empty_data_format(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.read(
                storage_connector=None,
                data_format="",
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

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
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert mock_python_engine_read_s3.call_count == 0

    def test_read_hopsfs_connector_empty_dataframe(self, mocker):
        # Arrange

        # Setting list of empty dataframes as return value from _read_hopsfs
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs",
            return_value=[pd.DataFrame(), pd.DataFrame()],
        )

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        dataframe = python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert isinstance(dataframe, pd.DataFrame)
        assert len(dataframe) == 0

    def test_read_hopsfs_connector_empty_dataframe_polars(self, mocker):
        # Arrange

        # Setting empty list as return value from _read_hopsfs
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs",
            return_value=[pl.DataFrame(), pl.DataFrame()],
        )

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        dataframe = python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="polars",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert isinstance(dataframe, pl.DataFrame)
        assert len(dataframe) == 0

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
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
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
                data_format="csv",
                read_options=None,
                location=None,
                dataframe_type="default",
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

    def test_read_polars_csv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_polars(data_format="csv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_polars_tsv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_polars(data_format="tsv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_polars_parquet(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        mock_obj = mocker.Mock()
        mock_obj.read.return_value = bytes()

        # Act
        python_engine._read_polars(data_format="parquet", obj=mock_obj)

        # Assert
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 1

    def test_read_polars_other(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._read_polars(data_format="ocr", obj=None)

        # Assert
        assert (
            str(e_info.value)
            == "ocr training dataset format is not supported to read as polars dataframe."
        )
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_hopsfs(self, mocker):
        # Arrange
        mock_python_engine_read_hopsfs_remote = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_remote"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_remote.call_count == 1

    def test_read_hopsfs_pydoop(self, mocker):
        # Arrange
        mymodule = mocker.Mock()
        mymodule.hdfs.return_value = mocker.Mock()
        mymodule.hdfs.ls.return_value = ["path_1", "path_2"]
        mymodule.hdfs.path.isfile.return_value = False
        mymodule.hdfs.path.getsize.return_value = 0
        mocker.patch.dict("sys.modules", pydoop=mymodule)
        mock_python_engine_read_hopsfs_remote = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_remote"
        )
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mymodule.call_count == 0
        assert mock_python_engine_read_hopsfs_remote.call_count == 0
        assert mock_python_engine_read_pandas.call_count == 0

    def test_read_hopsfs_pydoop_isfile(self, mocker):
        # Arrange
        mymodule = mocker.Mock()
        mymodule.hdfs.return_value = mocker.Mock()
        mymodule.hdfs.ls.return_value = ["path_1", "path_2"]
        mymodule.hdfs.path.isfile.return_value = True
        mymodule.hdfs.path.getsize.return_value = 0
        mocker.patch.dict("sys.modules", pydoop=mymodule)
        mock_python_engine_read_hopsfs_remote = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_remote"
        )
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_remote.call_count == 0
        assert mock_python_engine_read_pandas.call_count == 0

    def test_read_hopsfs_pydoop_getsize(self, mocker):
        # Arrange
        mymodule = mocker.Mock()
        mymodule.hdfs.return_value = mocker.Mock()
        mymodule.hdfs.ls.return_value = ["path_1", "path_2"]
        mymodule.hdfs.path.isfile.return_value = False
        mymodule.hdfs.path.getsize.return_value = 100
        mocker.patch.dict("sys.modules", pydoop=mymodule)
        mock_python_engine_read_hopsfs_remote = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_remote"
        )
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_remote.call_count == 0
        assert mock_python_engine_read_pandas.call_count == 0

    def test_read_hopsfs_pydoop_isfile_getsize(self, mocker):
        # Arrange
        mymodule = mocker.Mock()
        mymodule.hdfs.return_value = mocker.Mock()
        mymodule.hdfs.ls.return_value = ["path_1", "path_2"]
        mymodule.hdfs.path.isfile.return_value = True
        mymodule.hdfs.path.getsize.return_value = 100
        mocker.patch.dict("sys.modules", pydoop=mymodule)
        mock_python_engine_read_hopsfs_remote = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_remote"
        )
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_remote.call_count == 0
        assert mock_python_engine_read_pandas.call_count == 2

    def test_read_hopsfs_remote(self, mocker):
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
        python_engine._read_hopsfs_remote(location=None, data_format=None)

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

    def test_cast_columns(self, mocker):
        class LabelIndex:
            def __init__(self, label, index):
                self.label = label
                self.index = index

        python_engine = python.Engine()
        d = {
            "string": ["s", "s"],
            "bigint": [1, 2],
            "int": [1, 2],
            "smallint": [1, 2],
            "tinyint": [1, 2],
            "float": [1.0, 2.2],
            "double": [1.0, 2.2],
            "timestamp": [1641340800000, 1641340800000],
            "boolean": ["False", None],
            "date": ["2022-01-27", "2022-01-28"],
            "binary": [b"1", b"2"],
            "array<string>": ["['123']", "['1234']"],
            "struc": ["{'label':'blue','index':45}", "{'label':'blue','index':46}"],
            "decimal": ["1.1", "1.2"],
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
        cast_df = python_engine.cast_columns(df, schema)
        arrow_schema = pa.Schema.from_pandas(cast_df)
        expected = {
            "string": object,
            "bigint": pd.Int64Dtype(),
            "int": pd.Int32Dtype(),
            "smallint": pd.Int16Dtype(),
            "tinyint": pd.Int8Dtype(),
            "float": np.dtype("float32"),
            "double": np.dtype("float64"),
            "timestamp": np.dtype("datetime64[ns]"),
            "boolean": object,
            "date": np.dtype(date),
            "binary": object,
            "array<string>": object,
            "struc": object,
            "decimal": np.dtype(decimal.Decimal),
        }
        assert pa.types.is_string(arrow_schema.field("string").type)
        assert pa.types.is_boolean(arrow_schema.field("boolean").type)
        assert pa.types.is_binary(arrow_schema.field("binary").type)
        assert pa.types.is_list(arrow_schema.field("array<string>").type)
        assert pa.types.is_struct(arrow_schema.field("struc").type)
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

    def test_profile_pandas(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
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
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    def test_profile_pandas_with_null_column(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, None], "col3": [None, None]}
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
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    def test_profile_polars(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pl.DataFrame(data=d)

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
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    def test_profile_polars_with_null_column(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, None], "col3": [None, None]}
        df = pl.DataFrame(data=d)

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
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    def test_profile_relevant_columns(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.return_value = {
            "dataType": "Integral",
            "test_key": "test_value",
        }

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
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
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 1

    def test_profile_relevant_columns_diff_dtypes(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=["col1", "col3"],
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 2

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
        result = python_engine._convert_pandas_statistics(stat=stat, dataType="Integer")

        # Assert
        assert result == {
            "dataType": "Integer",
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
            "count": 100,
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

    def test_convert_to_default_dataframe_pandas(self, mocker):
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

    def test_convert_to_default_dataframe_pandas_with_spaces(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        d = {"col 1": [1, 2], "co 2 co": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        assert result.columns.values.tolist() == ["col_1", "co_2_co"]
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains feature names with spaces: `['col 1', 'co 2 co']`. "
            "Feature names are sanitized to use underscore '_' in the feature store."
        )

    def test_convert_to_default_dataframe_polars(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        df = pl.DataFrame(
            [
                pl.Series("Col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "Date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone="Africa/Abidjan"),
                ),
            ]
        )

        # Resulting dataframe
        expected_converted_df = pl.DataFrame(
            [
                pl.Series("col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone=None),
                ),
            ]
        )

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        polars_assert_frame_equal(result, expected_converted_df)
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains upper case letters in feature names: `['Col1', 'Date']`. Feature names are sanitized to lower case in the feature store."
        )

    def test_parse_schema_feature_group_pandas(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._convert_pandas_dtype_to_offline_type")

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

    def test_parse_schema_feature_group_polars(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._convert_pandas_dtype_to_offline_type")

        python_engine = python.Engine()

        df = pl.DataFrame(
            [
                pl.Series("col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone="Africa/Abidjan"),
                ),
            ]
        )

        # Act
        result = python_engine.parse_schema_feature_group(
            dataframe=df, time_travel_format=None
        )
        print(result)

        # Assert
        assert len(result) == 3
        assert result[0].name == "col1"
        assert result[1].name == "col2"
        assert result[2].name == "date"

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

    def test_convert_simple_pandas_type_uint8(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.uint8()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_uint16(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.uint16()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int8(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.int8()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int16(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.int16()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_int32(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.int32()
        )

        # Assert
        assert result == "int"

    def test_convert_simple_pandas_type_uint32(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.uint32()
        )

        # Assert
        assert result == "bigint"

    def test_convert_simple_pandas_type_int64(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.int64()
        )

        # Assert
        assert result == "bigint"

    def test_convert_simple_pandas_type_float16(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.float16()
        )

        # Assert
        assert result == "float"

    def test_convert_simple_pandas_type_float32(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.float32()
        )

        # Assert
        assert result == "float"

    def test_convert_simple_pandas_type_float64(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.float64()
        )

        # Assert
        assert result == "double"

    def test_convert_simple_pandas_type_datetime64ns(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="ns")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64nstz(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="ns", tz="UTC")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64us(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="us")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64ustz(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="us", tz="UTC")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64ms(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="ms")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64mstz(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="ms", tz="UTC")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64s(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="s")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_datetime64stz(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.timestamp(unit="s", tz="UTC")
        )

        # Assert
        assert result == "timestamp"

    def test_convert_simple_pandas_type_bool(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.bool_()
        )

        # Assert
        assert result == "boolean"

    def test_convert_simple_pandas_type_category_unordered(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.dictionary(
                value_type=pa.string(), index_type=pa.int8(), ordered=False
            )
        )

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_large_string_category_unordered(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.dictionary(
                value_type=pa.large_string(), index_type=pa.int64(), ordered=False
            )
        )

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_large_string_category_ordered(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.dictionary(
                value_type=pa.large_string(), index_type=pa.int64(), ordered=True
            )
        )

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_category_ordered(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.dictionary(
                value_type=pa.string(), index_type=pa.int8(), ordered=True
            )
        )

        # Assert
        assert result == "string"

    def test_convert_simple_pandas_type_other(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(ValueError) as e_info:
            python_engine._convert_simple_pandas_dtype_to_offline_type(
                arrow_type="other"
            )

        # Assert
        assert str(e_info.value) == "dtype 'other' not supported"

    def test_infer_type_pyarrow_list(self):
        # Arrange

        python_engine = python.Engine()

        # Act
        result = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_type=pa.list_(pa.int8())
        )

        # Assert
        assert result == "array<int>"

    def test_infer_type_pyarrow_large_list(self):
        # Arrange

        python_engine = python.Engine()

        # Act
        result = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_type=pa.large_list(pa.int8())
        )

        # Assert
        assert result == "array<int>"

    def test_infer_type_pyarrow_struct(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_type=pa.struct([pa.field("f1", pa.int32())])
        )

        # Assert
        assert result == "struct<f1:int>"

    def test_infer_type_pyarrow_date32(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.date32()
        )

        # Assert
        assert result == "date"

    def test_infer_type_pyarrow_date64(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.date64()
        )

        # Assert
        assert result == "date"

    def test_infer_type_pyarrow_binary(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.binary()
        )

        # Assert
        assert result == "binary"

    def test_infer_type_pyarrow_large_binary(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.large_binary()
        )

        # Assert
        assert result == "binary"

    def test_infer_type_pyarrow_string(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.string()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_large_string(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.large_string()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_utf8(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine._convert_simple_pandas_dtype_to_offline_type(
            arrow_type=pa.utf8()
        )

        # Assert
        assert result == "string"

    def test_infer_type_pyarrow_other(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(ValueError) as e_info:
            python_engine._convert_simple_pandas_dtype_to_offline_type(
                arrow_type=pa.time32("s")
            )

        # Assert
        assert str(e_info.value) == "dtype 'time32[s]' not supported"

    def test_infer_type_pyarrow_struct_with_decimal_fields(self):
        # Arrange
        mapping = {f"user{i}": 2.0 for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "struct<user0:double,user1:double>"

    def test_infer_type_pyarrow_struct_with_decimal_and_string_fields(self):
        # Arrange
        mapping = {"user0": 2.0, "user1": "test"}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "struct<user0:double,user1:string>"

    def test_infer_type_pyarrow_struct_with_list_fields(self):
        # Arrange
        mapping = {"user0": list(np.random.normal(size=5)), "user1": ["test", "test"]}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "struct<user0:array<double>,user1:array<string>>"

    def test_infer_type_pyarrow_struct_with_string_fields(self):
        # Arrange
        mapping = {f"user{i}": "test" for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "struct<user0:string,user1:string>"

    def test_infer_type_pyarrow_struct_with_struct_fields(self):
        # Arrange
        mapping = {f"user{i}": {"value": "test"} for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert (
            arrow_type
            == "struct<user0:struct<value:string>,user1:struct<value:string>>"
        )

    def test_infer_type_pyarrow_struct_with_struct_fields_with_list_values(self):
        # Arrange
        mapping = {
            f"user{i}": {"value": list(np.random.normal(size=5))} for i in range(2)
        }
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert (
            arrow_type
            == "struct<user0:struct<value:array<double>>,user1:struct<value:array<double>>>"
        )

    def test_infer_type_pyarrow_struct_with_nested_struct_fields(self):
        # Arrange
        mapping = {f"user{i}": {"value": {"value": "test"}} for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert (
            arrow_type
            == "struct<user0:struct<value:struct<value:string>>,user1:struct<value:struct<value:string>>>"
        )

    def test_infer_type_pyarrow_list_of_struct_fields(self):
        # Arrange
        mapping = [{"value": np.random.normal(size=5)}]
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert arrow_type == "array<struct<value:array<double>>>"

    def test_infer_type_pyarrow_struct_with_list_of_struct_fields(self):
        # Arrange
        mapping = {f"user{i}": [{"value": np.random.normal(size=5)}] for i in range(2)}
        pdf = pd.DataFrame(
            data=zip(list(range(1, 2)), [mapping] * 2),
            columns=["id", "mapping"],
        )
        arrow_schema = pa.Schema.from_pandas(pdf)

        python_engine = python.Engine()

        # Act
        arrow_type = python_engine._convert_pandas_object_type_to_offline_type(
            arrow_schema.field("mapping").type
        )

        # Assert
        assert (
            arrow_type
            == "struct<user0:array<struct<value:array<double>>>,user1:array<struct<value:array<double>>>>"
        )

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
        mock_util_get_job_url = mocker.patch("hsfs.util.get_job_url")

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
        assert mock_util_get_job_url.call_count == 1

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
            dataframe_type="default",
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
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_prepare_transform_split_df.call_count == 1

    def test_split_labels(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels=None
        )

        # Assert
        assert str(result_df) == "   Col1  col2\n0     1     3\n1     2     4"
        assert str(result_df_split) == "None"

    def test_split_labels_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels=None
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert result_df_split is None

    def test_split_labels_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="pandas", labels=None
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert result_df_split is None

    def test_split_labels_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pl.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="polars", labels=None
        )

        # Assert
        assert isinstance(result_df, pl.DataFrame) or isinstance(
            result_df, pl.dataframe.frame.DataFrame
        )
        assert result_df_split is None

    def test_split_labels_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pd.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="python", labels=None
        )

        # Assert
        assert isinstance(result_df, list)
        assert result_df_split is None

    def test_split_labels_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pd.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="numpy", labels=None
        )

        # Assert
        assert isinstance(result_df, np.ndarray)
        assert result_df_split is None

    def test_split_labels_labels(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels="col1"
        )

        # Assert
        assert str(result_df) == "   col2\n0     3\n1     4"
        assert str(result_df_split) == "0    1\n1    2\nName: col1, dtype: int64"

    def test_split_labels_labels_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert isinstance(result_df_split, pd.Series)

    def test_split_labels_labels_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="pandas", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert isinstance(result_df_split, pd.Series)

    def test_split_labels_labels_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pl.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="polars", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pl.DataFrame) or isinstance(
            result_df, pl.dataframe.frame.DataFrame
        )
        assert isinstance(result_df_split, pl.Series)

    def test_split_labels_labels_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="python", labels="col1"
        )

        # Assert
        assert isinstance(result_df, list)
        assert isinstance(result_df_split, list)

    def test_split_labels_labels_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="numpy", labels="col1"
        )

        # Assert
        assert isinstance(result_df, np.ndarray)
        assert isinstance(result_df_split, np.ndarray)

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
            dataframe_type="default",
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
            dataframe_type="default",
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
            dataframe_type="default",
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

    def test_random_split_size_precision_1(self, mocker):
        # In python sum([0.6, 0.3, 0.1]) != 1.0 due to floating point precision.
        # This test checks if different split ratios can be handled.
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2] * 10, "col2": [3, 4] * 10}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.6, "validation": 0.3, "test": 0.1},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["train", "validation", "test"]
        for column in list(result):
            assert not result[column].empty

    def test_random_split_size_precision_2(self, mocker):
        # This test checks if the method can handle split ratio with high precision.
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2] * 10, "col2": [3, 4] * 10}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 1 / 3, "validation": 1 - 1 / 3 - 0.1, "test": 0.1},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["train", "validation", "test"]
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
        mocker.patch("hsfs.util.get_job_url")
        mock_python_engine_wait_for_job = mocker.patch(
            "hsfs.core.job.Job._wait_for_job"
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

    def test_write_training_dataset_query_td(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_job = mocker.patch("hsfs.core.job.Job")

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mock_td_api.return_value.compute.return_value = mock_job
        mocker.patch("hsfs.util.get_job_url")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        q = query.Query(fg, fg.features)

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
        assert mock_job._wait_for_job.call_count == 1

    def test_write_training_dataset_query_fv(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_job = mocker.patch("hsfs.core.job.Job")
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_api.return_value.compute_training_dataset.return_value = mock_job

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mocker.patch("hsfs.util.get_job_url")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        q = query.Query(fg, fg.features)

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
        assert mock_job._wait_for_job.call_count == 1

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

    def test_return_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pl.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="pandas"
        )

        # Assert
        assert isinstance(result, pl.DataFrame) or isinstance(
            result, pl.dataframe.frame.DataFrame
        )
        assert df.equals(result)

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

    def test_add_file(self):
        # Arrange
        python_engine = python.Engine()

        file = None

        # Act
        result = python_engine.add_file(file=file)

        # Assert
        assert result == file

    def test_apply_transformation_function_pandas(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        engine._engine_type = "python"
        python_engine = python.Engine()

        @hopsworks_udf(int)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    def test_apply_transformation_function_multiple_output(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        engine._engine_type = "python"
        python_engine = python.Engine()

        @hopsworks_udf([int, int])
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    def test_apply_transformation_function_multiple_input_output(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        engine._engine_type = "python"
        python_engine = python.Engine()

        @hopsworks_udf([int, int])
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["plus_two_col1-col2_0", "plus_two_col1-col2_1"])
        assert len(result) == 2
        assert result["plus_two_col1-col2_0"][0] == 2
        assert result["plus_two_col1-col2_0"][1] == 3
        assert result["plus_two_col1-col2_1"][0] == 12
        assert result["plus_two_col1-col2_1"][1] == 13

    def test_apply_transformation_function_polars(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")

        engine._engine_type = "python"
        python_engine = python.Engine()

        @hopsworks_udf(int)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pl.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

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
        fg.feature_store = mocker.Mock()

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
        fg.feature_store = mocker.Mock()

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

    def test_get_kafka_config(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_instance")
        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )

        json = backend_fixtures["storage_connector"]["get_kafka"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        mocker.patch("hsfs.engine.python.isinstance", return_value=True)

        mock_client = mocker.patch("hsfs.client.get_instance")
        mock_client.return_value._write_pem.return_value = (
            "test_ssl_ca_location",
            "test_ssl_certificate_location",
            "test_ssl_key_location",
        )

        python_engine = python.Engine()

        # Act
        result = python_engine._get_kafka_config(
            1,
            write_options={
                "kafka_producer_config": {"test_name_1": "test_value_1"},
            },
        )

        # Assert
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_args[0][1]
            is False
        )
        assert result == {
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.ca.location": "test_ssl_ca_location",
            "ssl.certificate.location": "test_ssl_certificate_location",
            "ssl.key.location": "test_ssl_key_location",
            "test_name_1": "test_value_1",
        }

    def test_get_kafka_config_external_client(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_instance")
        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )

        json = backend_fixtures["storage_connector"]["get_kafka"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        mocker.patch("hsfs.engine.python.isinstance", return_value=False)

        mock_client = mocker.patch("hsfs.client.get_instance")
        mock_client.return_value._write_pem.return_value = (
            "test_ssl_ca_location",
            "test_ssl_certificate_location",
            "test_ssl_key_location",
        )

        python_engine = python.Engine()

        # Act
        result = python_engine._get_kafka_config(
            1,
            write_options={
                "kafka_producer_config": {"test_name_1": "test_value_1"},
            },
        )

        # Assert
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_args[0][1]
            is True
        )
        assert result == {
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.ca.location": "test_ssl_ca_location",
            "ssl.certificate.location": "test_ssl_certificate_location",
            "ssl.key.location": "test_ssl_key_location",
            "test_name_1": "test_value_1",
        }

    def test_get_kafka_config_internal_kafka(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_instance")
        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )

        json = backend_fixtures["storage_connector"]["get_kafka"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        mocker.patch("hsfs.engine.python.isinstance", return_value=True)

        mock_client = mocker.patch("hsfs.client.get_instance")
        mock_client.return_value._write_pem.return_value = (
            "test_ssl_ca_location",
            "test_ssl_certificate_location",
            "test_ssl_key_location",
        )

        python_engine = python.Engine()

        # Act
        result = python_engine._get_kafka_config(
            1,
            write_options={
                "kafka_producer_config": {"test_name_1": "test_value_1"},
                "internal_kafka": True,
            },
        )

        # Assert
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_args[0][1]
            is False
        )
        assert result == {
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.ca.location": "test_ssl_ca_location",
            "ssl.certificate.location": "test_ssl_certificate_location",
            "ssl.key.location": "test_ssl_key_location",
            "test_name_1": "test_value_1",
        }

    def test_get_kafka_config_external_client_internal_kafka(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mocker.patch("hsfs.engine.get_instance")
        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )

        json = backend_fixtures["storage_connector"]["get_kafka"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        mocker.patch("hsfs.engine.python.isinstance", return_value=False)

        mock_client = mocker.patch("hsfs.client.get_instance")
        mock_client.return_value._write_pem.return_value = (
            "test_ssl_ca_location",
            "test_ssl_certificate_location",
            "test_ssl_key_location",
        )

        python_engine = python.Engine()

        # Act
        result = python_engine._get_kafka_config(
            1,
            write_options={
                "kafka_producer_config": {"test_name_1": "test_value_1"},
                "internal_kafka": True,
            },
        )

        # Assert
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_args[0][1]
            is False
        )
        assert result == {
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.ca.location": "test_ssl_ca_location",
            "ssl.certificate.location": "test_ssl_certificate_location",
            "ssl.key.location": "test_ssl_key_location",
            "test_name_1": "test_value_1",
        }

    def test_materialization_kafka(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.engine.python.Engine._get_encoder_func")
        mocker.patch("hsfs.engine.python.Engine._encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.engine.python.Engine._kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.engine.python.Engine._kafka_get_offsets",
            return_value=" tests_offsets",
        )
        mocker.patch(
            "hsfs.core.job_api.JobApi.last_execution",
            return_value=["", ""],
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
            time_travel_format="HUDI",
        )

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults",
            await_termination=False,
        )

    def test_materialization_kafka_first_job_execution(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.engine.python.Engine._get_encoder_func")
        mocker.patch("hsfs.engine.python.Engine._encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.engine.python.Engine._kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.engine.python.Engine._kafka_get_offsets",
            return_value=" tests_offsets",
        )
        mocker.patch(
            "hsfs.core.job_api.JobApi.last_execution",
            return_value=[],
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
            time_travel_format="HUDI",
        )

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults tests_offsets",
            await_termination=False,
        )
    
    def test_materialization_kafka_skip_offsets(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.engine.python.Engine._get_encoder_func")
        mocker.patch("hsfs.engine.python.Engine._encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.engine.python.Engine._kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.engine.python.Engine._kafka_get_offsets",
            return_value=" tests_offsets",
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
            time_travel_format="HUDI",
        )

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True, "skip_offsets": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults tests_offsets",
            await_termination=False,
        )

    def test_materialization_kafka_topic_doesnt_exist(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.python.Engine._get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.engine.python.Engine._get_encoder_func")
        mocker.patch("hsfs.engine.python.Engine._encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.engine.python.Engine._kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.engine.python.Engine._kafka_get_offsets",
            side_effect=["", " tests_offsets"],
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
            time_travel_format="HUDI",
        )

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults tests_offsets",
            await_termination=False,
        )

    def test_kafka_get_offsets_high(self, mocker):
        # Arrange
        topic_name = "test_topic"
        partition_metadata = PartitionMetadata()
        partition_metadata.id = 0
        topic_metadata = TopicMetadata()
        topic_metadata.partitions = {partition_metadata.id: partition_metadata}
        topic_mock = mocker.MagicMock()

        # return no topics and one commit, so it should start the job with the extra arg
        topic_mock.topics = {topic_name: topic_metadata}

        consumer = mocker.MagicMock()
        consumer.list_topics = mocker.MagicMock(return_value=topic_mock)
        consumer.get_watermark_offsets = mocker.MagicMock(return_value=(0, 11))
        mocker.patch(
            "hsfs.engine.python.Engine._init_kafka_consumer",
            return_value=consumer,
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
            time_travel_format="HUDI",
        )
        fg._online_topic_name = topic_name

        # Act
        result = python_engine._kafka_get_offsets(
            feature_group=fg,
            offline_write_options={},
            high=True,
        )

        # Assert
        assert result == f" -initialCheckPointString {topic_name},0:11"

    def test_kafka_get_offsets_low(self, mocker):
        # Arrange
        topic_name = "test_topic"
        partition_metadata = PartitionMetadata()
        partition_metadata.id = 0
        topic_metadata = TopicMetadata()
        topic_metadata.partitions = {partition_metadata.id: partition_metadata}
        topic_mock = mocker.MagicMock()

        # return no topics and one commit, so it should start the job with the extra arg
        topic_mock.topics = {topic_name: topic_metadata}

        consumer = mocker.MagicMock()
        consumer.list_topics = mocker.MagicMock(return_value=topic_mock)
        consumer.get_watermark_offsets = mocker.MagicMock(return_value=(0, 11))
        mocker.patch(
            "hsfs.engine.python.Engine._init_kafka_consumer",
            return_value=consumer,
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
            time_travel_format="HUDI",
        )
        fg._online_topic_name = topic_name

        # Act
        result = python_engine._kafka_get_offsets(
            feature_group=fg,
            offline_write_options={},
            high=False,
        )

        # Assert
        assert result == f" -initialCheckPointString {topic_name},0:0"

    def test_kafka_get_offsets_no_topic(self, mocker):
        # Arrange
        topic_name = "test_topic"
        topic_mock = mocker.MagicMock()

        # return no topics and one commit, so it should start the job with the extra arg
        topic_mock.topics = {}

        consumer = mocker.MagicMock()
        consumer.list_topics = mocker.MagicMock(return_value=topic_mock)
        consumer.get_watermark_offsets = mocker.MagicMock(return_value=(0, 11))
        mocker.patch(
            "hsfs.engine.python.Engine._init_kafka_consumer",
            return_value=consumer,
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
            time_travel_format="HUDI",
        )
        fg._online_topic_name = topic_name

        # Act
        result = python_engine._kafka_get_offsets(
            feature_group=fg,
            offline_write_options={},
            high=True,
        )

        # Assert
        assert result == ""

    def test_test(self, mocker):
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "topic_name"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        assert fg.materialization_job.config == {"defaultArgs": "defaults"}
