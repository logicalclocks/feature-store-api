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

import boto3
import pandas as pd
from hsfs.core import arrow_flight_client, inode
from hsfs.engine import python
from hsfs.storage_connector import S3Connector
from moto import mock_aws


class TestPythonReader:
    def test_read_s3_parquet(self, mocker, dataframe_fixture_basic):
        python_engine = python.Engine()

        with mock_aws():
            s3 = boto3.client(
                "s3",
                aws_access_key_id="",
                aws_secret_access_key="",
            )
            conn = boto3.resource("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="test-parquet-reading")
            with open("python/tests/data/test_basic.parquet", "rb") as data:
                s3.upload_fileobj(data, "test-parquet-reading", "test_basic.parquet")
            connector = S3Connector(
                1,
                "test",
                1,
                description="Test",
                # members specific to type of connector
                access_key=None,
                secret_key=None,
                server_encryption_algorithm=None,
                server_encryption_key=None,
                bucket="test-parquet-reading",
                session_token=None,
            )

            # Act
            df_list = python_engine._read_s3(
                connector, "s3://test-parquet-reading/test_basic.parquet", "parquet"
            )

        # Assert
        assert len(df_list) == 1
        assert dataframe_fixture_basic.equals(df_list[0])

    def test_read_s3_csv(self, mocker, dataframe_fixture_basic):
        python_engine = python.Engine()

        with mock_aws():
            s3 = boto3.client(
                "s3",
                aws_access_key_id="",
                aws_secret_access_key="",
            )
            conn = boto3.resource("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="test-csv-reading")
            with open("python/tests/data/test_basic.csv", "rb") as data:
                s3.upload_fileobj(data, "test-csv-reading", "test_basic.csv")
            connector = S3Connector(
                1,
                "test",
                1,
                description="Test",
                # members specific to type of connector
                access_key=None,
                secret_key=None,
                server_encryption_algorithm=None,
                server_encryption_key=None,
                bucket="test-csv-reading",
                session_token=None,
            )

            # Act
            df_list = python_engine._read_s3(
                connector, "s3://test-csv-reading/test_basic.csv", "csv"
            )

        df_list[0]["event_date"] = pd.to_datetime(
            df_list[0]["event_date"], format="%Y-%m-%d"
        ).dt.date

        # Assert
        assert len(df_list) == 1
        assert dataframe_fixture_basic.equals(df_list[0])

    def test_read_pandas_parquet(self, dataframe_fixture_basic):
        df = python.Engine()._read_pandas(
            "parquet", "python/tests/data/test_basic.parquet"
        )

        # Assert
        assert dataframe_fixture_basic.equals(df)

    def test_read_pandas_csv(self, dataframe_fixture_basic):
        df = python.Engine()._read_pandas("csv", "python/tests/data/test_basic.csv")

        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y-%m-%d").dt.date

        # Assert
        assert dataframe_fixture_basic.equals(df)

    def test_read_pandas_tsv(self, dataframe_fixture_basic):
        df = python.Engine()._read_pandas("tsv", "python/tests/data/test_basic.tsv")

        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y-%m-%d").dt.date

        # Assert
        assert dataframe_fixture_basic.equals(df)

    def test_read_hopsfs_remote_parquet(self, mocker, dataframe_fixture_basic):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")
        i = inode.Inode(attributes={"path": "test_path"})
        mock_dataset_api.return_value.list_files.return_value = (0, [i])
        with open(
            "python/tests/data/test_basic.parquet", mode="rb"
        ) as file:  # b is important -> binary
            mock_dataset_api.return_value.read_content.return_value.content = (
                file.read()
            )
        arrow_flight_client.get_instance()._disabled_for_session = True
        arrow_flight_client.get_instance()._enabled_on_cluster = False

        # Act
        df_list = python.Engine()._read_hopsfs_remote(
            location=None, data_format="parquet"
        )

        # Assert
        assert len(df_list) == 1
        assert dataframe_fixture_basic.equals(df_list[0])

    def test_read_hopsfs_remote_csv(self, mocker, dataframe_fixture_basic):
        # Arrange
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")
        i = inode.Inode(attributes={"path": "test_path"})
        mock_dataset_api.return_value.list_files.return_value = (0, [i])
        with open(
            "python/tests/data/test_basic.csv", mode="rb"
        ) as file:  # b is important -> binary
            mock_dataset_api.return_value.read_content.return_value.content = (
                file.read()
            )

        # Act
        df_list = python.Engine()._read_hopsfs_remote(location=None, data_format="csv")
        df_list[0]["event_date"] = pd.to_datetime(
            df_list[0]["event_date"], format="%Y-%m-%d"
        ).dt.date

        # Assert
        assert len(df_list) == 1
        assert dataframe_fixture_basic.equals(df_list[0])
