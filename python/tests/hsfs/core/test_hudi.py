#
#   Copyright 2022 Logical Clocks AB
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

import unittest
from unittest.mock import patch, Mock

from python.hsfs import feature_group, engine, client, util
from python.hsfs.core import hudi_engine, feature_group_api, storage_connector_api


class TestHudi(unittest.TestCase):
    def test_save_hudi_fg(self):
        with patch.object(
            hudi_engine.HudiEngine,
            "_write_hudi_dataset",
        ) as mock_hudi, patch.object(
            feature_group_api.FeatureGroupApi, "commit"
        ) as mock_feature_group_api:
            # Arrange
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=None,
                spark_context=None,
                spark_session=None,
            )

            # Act
            hudiEngine.save_hudi_fg(
                dataset=None,
                save_mode=None,
                operation=None,
                write_options=None,
                validation_id=None,
            )

            # Assert
            self.assertEqual(1, mock_hudi.call_count)
            self.assertEqual(1, mock_feature_group_api.call_count)
            self.assertEqual(
                None, mock_feature_group_api.call_args.args[1].validation_id
            )

    def test_save_hudi_fg_validation_id(self):
        with patch.object(
            hudi_engine.HudiEngine,
            "_write_hudi_dataset",
        ) as mock_hudi, patch.object(
            feature_group_api.FeatureGroupApi, "commit"
        ) as mock_feature_group_api:
            # Arrange
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=None,
                spark_context=None,
                spark_session=None,
            )

            # Act
            hudiEngine.save_hudi_fg(
                dataset=None,
                save_mode=None,
                operation=None,
                write_options=None,
                validation_id=1,
            )

            # Assert
            self.assertEqual(1, mock_hudi.call_count)
            self.assertEqual(1, mock_feature_group_api.call_count)
            self.assertEqual(1, mock_feature_group_api.call_args.args[1].validation_id)

    def test_delete_record(self):
        with patch.object(
            hudi_engine.HudiEngine,
            "_write_hudi_dataset",
        ) as mock_hudi, patch.object(
            feature_group_api.FeatureGroupApi, "commit"
        ) as mock_feature_group_api:
            # Arrange
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=None,
                spark_context=None,
                spark_session=None,
            )

            # Act
            hudiEngine.delete_record(delete_df=None, write_options={})

            # Assert
            self.assertEqual(1, mock_hudi.call_count)
            self.assertEqual(1, mock_feature_group_api.call_count)
            self.assertIn(
                "hoodie.datasource.write.payload.class", mock_hudi.call_args.args[3]
            )

    def test_register_temporary_table(self):
        with patch.object(
            hudi_engine.HudiEngine,
            "_setup_hudi_read_opts",
        ) as mock_hudi, patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            spark_session = Mock()
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=fg,
                spark_context=None,
                spark_session=spark_session,
            )

            # Act
            hudiEngine.register_temporary_table(hudi_fg_alias=Mock(), read_options=None)

            # Assert
            self.assertEqual(1, mock_hudi.call_count)
            self.assertTrue(spark_session.read.format.called)

    def test_write_hudi_dataset(self):
        with patch.object(
            hudi_engine.HudiEngine,
            "_setup_hudi_write_opts",
        ) as mock_hudi, patch.object(
            hudi_engine.HudiEngine, "_get_last_commit_metadata"
        ), patch.object(
            engine, "get_type"
        ):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=fg,
                spark_context=None,
                spark_session=None,
            )

            # Act
            dataset = Mock()
            hudiEngine._write_hudi_dataset(
                dataset=dataset, save_mode=None, operation=None, write_options=None
            )

            # Assert
            self.assertEqual(1, mock_hudi.call_count)
            self.assertTrue(dataset.write.format.called)

    def test_setup_hudi_write_opts(self):
        with patch.object(
            hudi_engine.HudiEngine,
            "_get_conn_str",
        ) as mock_hudi, patch.object(engine, "get_type"):
            # Arrange
            mock_hudi.return_value = "jdbcurl"
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=["key1", "key2"],
                partition_key=["key3", "key4"],
                id=0,
            )
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=fg,
                spark_context=None,
                spark_session=None,
            )

            # Act
            results = hudiEngine._setup_hudi_write_opts(
                operation="test", write_options={}
            )

            # Assert
            self.assertEqual(1, mock_hudi.call_count)
            self.assertEqual(
                {
                    "hoodie.bulkinsert.shuffle.parallelism": "5",
                    "hoodie.datasource.hive_sync.database": None,
                    "hoodie.datasource.hive_sync.enable": "true",
                    "hoodie.datasource.hive_sync.jdbcurl": "jdbcurl",
                    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
                    "hoodie.datasource.hive_sync.partition_fields": "key3,key4",
                    "hoodie.datasource.hive_sync.support_timestamp": "true",
                    "hoodie.datasource.hive_sync.table": "test_1",
                    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.CustomKeyGenerator",
                    "hoodie.datasource.write.operation": "test",
                    "hoodie.datasource.write.partitionpath.field": "key3:SIMPLE,key4:SIMPLE",
                    "hoodie.datasource.write.precombine.field": "key1",
                    "hoodie.datasource.write.recordkey.field": "key1,key2",
                    "hoodie.insert.shuffle.parallelism": "5",
                    "hoodie.table.name": "test_1",
                    "hoodie.upsert.shuffle.parallelism": "5",
                },
                results,
            )

    def test_setup_hudi_read_opts_with_timestamp(self):
        with patch.object(
            util,
            "get_hudi_datestr_from_timestamp",
        ) as mock_util, patch.object(engine, "get_type"):
            # Arrange
            mock_util.side_effect = [1, 2]
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=fg,
                spark_context=None,
                spark_session=None,
            )
            hudi_fg = Mock()

            # Act
            results = hudiEngine._setup_hudi_read_opts(
                hudi_fg_alias=hudi_fg, read_options={}
            )

            # Assert
            self.assertEqual(2, mock_util.call_count)
            self.assertEqual(
                {
                    "hoodie.datasource.query.type": "incremental",
                    "hoodie.datasource.read.begin.instanttime": 1,
                    "hoodie.datasource.read.end.instanttime": 2,
                },
                results,
            )

    def test_setup_hudi_read_opts(self):
        with patch.object(engine, "get_type"):
            # Arrange
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                partition_key=[],
                id=0,
            )
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=fg,
                spark_context=None,
                spark_session=None,
            )
            hudi_fg = Mock()
            hudi_fg.left_feature_group_end_timestamp = None

            # Act
            results = hudiEngine._setup_hudi_read_opts(
                hudi_fg_alias=hudi_fg, read_options={}
            )

            # Assert
            self.assertEqual({"hoodie.datasource.query.type": "snapshot"}, results)

    def test_get_last_commit_metadata(self):
        with patch.object(
            util,
            "get_timestamp_from_date_string",
        ) as mock_util:
            # Arrange
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name=None,
                feature_group=None,
                spark_context=None,
                spark_session=None,
            )
            spark_context = Mock()
            spark_context._jvm.org.apache.hudi.HoodieDataSourceHelpers.allCompletedCommitsCompactions().lastInstant().get().getTimestamp.return_value = (
                1
            )
            spark_context._jvm.org.apache.hudi.common.model.HoodieCommitMetadata.fromBytes().fetchTotalInsertRecordsWritten.return_value = (
                2
            )
            spark_context._jvm.org.apache.hudi.common.model.HoodieCommitMetadata.fromBytes().fetchTotalUpdateRecordsWritten.return_value = (
                3
            )
            spark_context._jvm.org.apache.hudi.common.model.HoodieCommitMetadata.fromBytes().getTotalRecordsDeleted.return_value = (
                4
            )
            mock_util.return_value = 5

            # Act
            results = hudiEngine._get_last_commit_metadata(
                spark_context=spark_context, base_path=""
            )

            # Assert
            self.assertEqual(1, mock_util.call_count)
            self.assertEqual(None, results.commitid)
            self.assertEqual(1, results.commit_date_string)
            self.assertEqual(2, results.rows_inserted)
            self.assertEqual(3, results.rows_updated)
            self.assertEqual(4, results.rows_deleted)
            self.assertEqual(5, results.commit_time)

    def test_get_conn_str_connstr(self):
        with patch.object(
            storage_connector_api.StorageConnectorApi, "get"
        ) as mock_storage_connector_api, patch.object(
            client, "get_instance"
        ) as mock_client:
            # Arrange
            hudiEngine = hudi_engine.HudiEngine(
                feature_store_id=99,
                feature_store_name="test",
                feature_group=None,
                spark_context=None,
                spark_session=None,
            )

            mock_client.return_value._get_jks_trust_store_path.return_value = "1"
            mock_client.return_value._cert_key = "2"
            mock_client.return_value._get_jks_key_store_path.return_value = "3"
            mock_client.return_value._cert_key = "4"
            hudiEngine._connstr = "test_url;"

            # Act
            results = hudiEngine._get_conn_str()

            # Assert
            self.assertEqual(
                "test_url;sslTrustStore=1;trustStorePassword=4;sslKeyStore=3;keyStorePassword=4",
                results,
            )

    def test_get_conn_str(self):
        # Arrange
        hudiEngine = hudi_engine.HudiEngine(
            feature_store_id=99,
            feature_store_name=None,
            feature_group=None,
            spark_context=None,
            spark_session=None,
        )

        # Act
        results = hudiEngine._get_conn_str()

        # Assert
        self.assertEqual("", results)
