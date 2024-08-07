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

import asyncio
from datetime import date, datetime

import pytest
import pytz
from hsfs import util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.embedding import EmbeddingFeature, EmbeddingIndex
from hsfs.feature import Feature
from mock import patch


class TestUtil:
    def test_get_hudi_datestr_from_timestamp(self):
        dt = util.get_hudi_datestr_from_timestamp(1640995200000)
        assert dt == "20220101000000000"

    def test_convert_event_time_to_timestamp_timestamp(self):
        dt = util.convert_event_time_to_timestamp(1640995200)
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_datetime(self):
        dt = util.convert_event_time_to_timestamp(datetime(2022, 1, 1, 0, 0, 0))
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_datetime_tz(self):
        dt = util.convert_event_time_to_timestamp(
            pytz.timezone("US/Pacific").localize(datetime(2021, 12, 31, 16, 0, 0))
        )
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_date(self):
        dt = util.convert_event_time_to_timestamp(date(2022, 1, 1))
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_string(self):
        dt = util.convert_event_time_to_timestamp("2022-01-01 00:00:00")
        assert dt == 1640995200000

    def test_convert_iso_event_time_to_timestamp_string(self):
        dt = util.convert_event_time_to_timestamp("2022-01-01T00:00:00.000000Z")
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd(self):
        timestamp = util.get_timestamp_from_date_string("2022-01-01")
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh(self):
        timestamp = util.get_timestamp_from_date_string("2022-01-01 00")
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm(self):
        timestamp = util.get_timestamp_from_date_string("2022-01-01 00:00")
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss(self):
        timestamp = util.get_timestamp_from_date_string("2022-01-01 00:00:00")
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss_f(self):
        timestamp = util.get_timestamp_from_date_string("2022-01-01 00:00:00.000")
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss_error(self):
        with pytest.raises(ValueError):
            util.get_timestamp_from_date_string("2022-13-01 00:00:00")

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss_error2(self):
        with pytest.raises(ValueError):
            util.get_timestamp_from_date_string("202-13-01 00:00:00")

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss_error3(self):
        with pytest.raises(ValueError):
            util.get_timestamp_from_date_string("00:00:00 2022-01-01")

    def test_convert_hudi_commit_time_to_timestamp(self):
        timestamp = util.get_timestamp_from_date_string("20221118095233099")
        assert timestamp == 1668765153099

    def test_get_dataset_type_HIVEDB(self):
        db_type = util.get_dataset_type(
            "/apps/hive/warehouse/temp_featurestore.db/storage_connector_resources/kafka__tstore.jks"
        )
        assert db_type == "HIVEDB"

    def test_get_dataset_type_HIVEDB_with_dfs(self):
        db_type = util.get_dataset_type(
            "hdfs:///apps/hive/warehouse/temp_featurestore.db/storage_connector_resources/kafka__tstore.jks"
        )
        assert db_type == "HIVEDB"

    def test_get_dataset_type_DATASET(self):
        db_type = util.get_dataset_type("/Projects/temp/Resources/kafka__tstore.jks")
        assert db_type == "DATASET"

    def test_get_dataset_type_DATASET_with_dfs(self):
        db_type = util.get_dataset_type(
            "hdfs:///Projects/temp/Resources/kafka__tstore.jks"
        )
        assert db_type == "DATASET"

    def test_get_job_url(self, mocker):
        # Arrange
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")

        # Act
        util.get_job_url(href="1/2/3/4/5/6/7/8")

        # Assert
        assert (
            mock_client_get_instance.return_value.replace_public_host.call_args[0][
                0
            ].path
            == "p/5/jobs/named/7/executions"
        )

    def test_get_feature_group_url(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_util_get_hostname_replaced_url = mocker.patch(
            "hsfs.util.get_hostname_replaced_url"
        )
        mock_client_get_instance.return_value._project_id = 50

        # Act
        util.get_feature_group_url(
            feature_group_id=feature_group_id, feature_store_id=feature_store_id
        )

        # Assert
        assert mock_util_get_hostname_replaced_url.call_count == 1
        assert (
            mock_util_get_hostname_replaced_url.call_args[0][0] == "/p/50/fs/99/fg/10"
        )

    def test_valid_embedding_type(self):
        embedding_index = EmbeddingIndex(
            features=[
                EmbeddingFeature("feature1", 3),
                EmbeddingFeature("feature2", 3),
                EmbeddingFeature("feature3", 3),
                EmbeddingFeature("feature4", 3),
            ]
        )
        # Define a schema with valid feature types
        schema = [
            Feature(name="feature1", type="array<int>"),
            Feature(name="feature2", type="array<bigint>"),
            Feature(name="feature3", type="array<float>"),
            Feature(name="feature4", type="array<double>"),
        ]
        # Call the method and expect no exceptions
        util.validate_embedding_feature_type(embedding_index, schema)

    def test_invalid_embedding_type(self):
        embedding_index = EmbeddingIndex(
            features=[
                EmbeddingFeature("feature1", 3),
                EmbeddingFeature("feature2", 3),
            ]
        )
        # Define a schema with an invalid feature type
        schema = [
            Feature(name="feature1", type="array<int>"),
            Feature(name="feature2", type="array<string>"),  # Invalid type
        ]
        # Call the method and expect a FeatureStoreException
        with pytest.raises(FeatureStoreException):
            util.validate_embedding_feature_type(embedding_index, schema)

    def test_missing_embedding_index(self):
        # Define a schema without an embedding index
        schema = [
            Feature(name="feature1", type="array<int>"),
            Feature(name="feature2", type="array<bigint>"),
        ]
        # Call the method with an empty feature_group (no embedding index)
        util.validate_embedding_feature_type(None, schema)
        # No exception should be raised

    def test_empty_schema(self):
        embedding_index = EmbeddingIndex(
            features=[
                EmbeddingFeature("feature1", 3),
                EmbeddingFeature("feature2", 3),
            ]
        )
        # Define an empty schema
        schema = []
        # Call the method with an empty schema
        util.validate_embedding_feature_type(embedding_index, schema)
        # No exception should be raised

    def test_create_async_engine(self, mocker):
        # Test when get_running_loop() raises a RuntimeError
        with patch("asyncio.get_running_loop", side_effect=RuntimeError):
            # mock storage connector
            online_connector = patch.object(util, "get_online_connector")
            with pytest.raises(
                RuntimeError,
                match="Event loop is not running. Please invoke this co-routine from a running loop or provide an event loop.",
            ):
                asyncio.run(util.create_async_engine(online_connector, True, 1))
