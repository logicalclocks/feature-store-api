#
#   Copyright 2021 Logical Clocks AB
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

from hsfs import feature_group, engine, client, util
from hsfs.constructor import hudi_feature_group_alias
from hsfs.core import hudi_engine, feature_group_api, storage_connector_api


class TestHudiEngine:
    def test_save_hudi_fg(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_hudi_engine_write_hudi_dataset = mocker.patch(
            "hsfs.core.hudi_engine.HudiEngine._write_hudi_dataset"
        )
        mock_fg_api_commit = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi.commit"
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=None,
            spark_context=None,
            spark_session=None,
        )

        # Act
        h_engine.save_hudi_fg(
            dataset=None,
            save_mode=None,
            operation=None,
            write_options=None,
            validation_id=10,
        )

        # Assert
        assert mock_hudi_engine_write_hudi_dataset.call_count == 1
        assert mock_fg_api_commit.call_count == 1
        assert mock_fg_api_commit.call_args.args[1].validation_id == 10

    def test_delete_record(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_hudi_engine_write_hudi_dataset = mocker.patch(
            "hsfs.core.hudi_engine.HudiEngine._write_hudi_dataset"
        )
        mock_fg_api_commit = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi.commit"
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=None,
            spark_context=None,
            spark_session=None,
        )

        # Act
        h_engine.delete_record(delete_df=None, write_options={})

        # Assert
        assert mock_hudi_engine_write_hudi_dataset.call_count == 1
        assert mock_fg_api_commit.call_count == 1
        assert (
            "hoodie.datasource.write.payload.class"
            in mock_hudi_engine_write_hudi_dataset.call_args.args[3]
        )
        assert mock_hudi_engine_write_hudi_dataset.call_args.args[1] == "append"

    def test_register_temporary_table(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_hudi_engine_setup_hudi_read_opts = mocker.patch(
            "hsfs.core.hudi_engine.HudiEngine._setup_hudi_read_opts"
        )

        spark_session = mocker.Mock()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            id=10,
            location="test",
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=fg,
            spark_context=None,
            spark_session=spark_session,
        )

        hudi_fg_alias = hudi_feature_group_alias.HudiFeatureGroupAlias(
            feature_group="", alias=None
        )

        # Act
        h_engine.register_temporary_table(
            hudi_fg_alias=hudi_fg_alias, read_options=None
        )

        # Assert
        assert mock_hudi_engine_setup_hudi_read_opts.call_count == 1
        assert spark_session.read.format.call_args.args[0] == h_engine.HUDI_SPARK_FORMAT
        assert (
            spark_session.read.format.return_value.options.return_value.load.call_args.args[
                0
            ]
            == fg.location
        )
        assert (
            spark_session.read.format.return_value.options.return_value.load.return_value.createOrReplaceTempView.call_args.args[
                0
            ]
            == hudi_fg_alias.alias
        )

    def test_write_hudi_dataset(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_hudi_engine_setup_hudi_write_opts = mocker.patch(
            "hsfs.core.hudi_engine.HudiEngine._setup_hudi_write_opts"
        )
        mock_hudi_engine_get_last_commit_metadata = mocker.patch(
            "hsfs.core.hudi_engine.HudiEngine._get_last_commit_metadata"
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            id=10,
            location="test",
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=fg,
            spark_context=None,
            spark_session=None,
        )

        dataset = mocker.Mock()

        # Act
        h_engine._write_hudi_dataset(
            dataset=dataset, save_mode="test", operation=None, write_options=None
        )

        # Assert
        assert mock_hudi_engine_setup_hudi_write_opts.call_count == 1
        assert mock_hudi_engine_get_last_commit_metadata.call_count == 1
        assert dataset.write.format.call_args.args[0] == h_engine.HUDI_SPARK_FORMAT
        assert (
            dataset.write.format.return_value.options.return_value.mode.call_args.args[
                0
            ]
            == "test"
        )
        assert (
            dataset.write.format.return_value.options.return_value.mode.return_value.save.call_args.args[
                0
            ]
            == fg.location
        )

    def test_write_hudi_dataset(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_hudi_engine_get_conn_str = mocker.patch(
            "hsfs.core.hudi_engine.HudiEngine._get_conn_str"
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["key1", "key2"],
            partition_key=["key3", "key4"],
            hudi_precombine_key=[],
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=fg,
            spark_context=None,
            spark_session=None,
        )

        mock_hudi_engine_get_conn_str.return_value = "test_conn_str"

        # Act
        result = h_engine._setup_hudi_write_opts(
            operation="test", write_options={"test_name": "test_value"}
        )

        # Assert
        assert mock_hudi_engine_get_conn_str.call_count == 1
        assert result == {
            "hoodie.bulkinsert.shuffle.parallelism": "5",
            "hoodie.datasource.hive_sync.database": None,
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.jdbcurl": "test_conn_str",
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
            "test_name": "test_value",
        }

    def test_write_hudi_dataset_hudi_precombine_key(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_hudi_engine_get_conn_str = mocker.patch(
            "hsfs.core.hudi_engine.HudiEngine._get_conn_str"
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["key1", "key2"],
            partition_key=["key3", "key4"],
            hudi_precombine_key="key2",
            time_travel_format="HUDI",
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=fg,
            spark_context=None,
            spark_session=None,
        )

        mock_hudi_engine_get_conn_str.return_value = "test_conn_str"

        # Act
        result = h_engine._setup_hudi_write_opts(
            operation="test", write_options={"test_name": "test_value"}
        )

        # Assert
        assert mock_hudi_engine_get_conn_str.call_count == 1
        assert result == {
            "hoodie.bulkinsert.shuffle.parallelism": "5",
            "hoodie.datasource.hive_sync.database": None,
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.jdbcurl": "test_conn_str",
            "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
            "hoodie.datasource.hive_sync.partition_fields": "key3,key4",
            "hoodie.datasource.hive_sync.support_timestamp": "true",
            "hoodie.datasource.hive_sync.table": "test_1",
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.CustomKeyGenerator",
            "hoodie.datasource.write.operation": "test",
            "hoodie.datasource.write.partitionpath.field": "key3:SIMPLE,key4:SIMPLE",
            "hoodie.datasource.write.precombine.field": "key2",
            "hoodie.datasource.write.recordkey.field": "key1,key2",
            "hoodie.insert.shuffle.parallelism": "5",
            "hoodie.table.name": "test_1",
            "hoodie.upsert.shuffle.parallelism": "5",
            "test_name": "test_value",
        }

    def test_setup_hudi_read_opts(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_util_get_hudi_datestr_from_timestamp = mocker.patch(
            "hsfs.util.get_hudi_datestr_from_timestamp"
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=None,
            spark_context=None,
            spark_session=None,
        )

        hudi_fg_alias = hudi_feature_group_alias.HudiFeatureGroupAlias(
            feature_group="",
            alias=None,
            left_feature_group_end_timestamp=None,
            left_feature_group_start_timestamp=0,
        )

        mock_util_get_hudi_datestr_from_timestamp.side_effect = [1, 2]

        # Act
        result = h_engine._setup_hudi_read_opts(
            hudi_fg_alias=hudi_fg_alias, read_options={"test_name": "test_value"}
        )

        # Assert
        assert mock_util_get_hudi_datestr_from_timestamp.call_count == 0
        assert result == {
            "hoodie.datasource.query.type": "snapshot",
            "test_name": "test_value",
        }

    def test_setup_hudi_read_opts_timestamp(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_util_get_hudi_datestr_from_timestamp = mocker.patch(
            "hsfs.util.get_hudi_datestr_from_timestamp"
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=None,
            spark_context=None,
            spark_session=None,
        )

        hudi_fg_alias = hudi_feature_group_alias.HudiFeatureGroupAlias(
            feature_group="",
            alias=None,
            left_feature_group_end_timestamp=2,
            left_feature_group_start_timestamp=1,
        )

        mock_util_get_hudi_datestr_from_timestamp.side_effect = [1, 2]

        # Act
        result = h_engine._setup_hudi_read_opts(
            hudi_fg_alias=hudi_fg_alias, read_options={"test_name": "test_value"}
        )

        # Assert
        assert mock_util_get_hudi_datestr_from_timestamp.call_count == 2
        assert result == {
            "hoodie.datasource.query.type": "incremental",
            "hoodie.datasource.read.begin.instanttime": 1,
            "hoodie.datasource.read.end.instanttime": 2,
            "test_name": "test_value",
        }

    def test_get_last_commit_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_util_get_timestamp_from_date_string = mocker.patch(
            "hsfs.util.get_timestamp_from_date_string"
        )

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=None,
            spark_context=None,
            spark_session=None,
        )

        spark_context = mocker.Mock()
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
        mock_util_get_timestamp_from_date_string.return_value = 5

        # Act
        result = h_engine._get_last_commit_metadata(
            spark_context=spark_context, base_path=None
        )

        # Assert
        assert mock_util_get_timestamp_from_date_string.call_count == 1
        assert result.commitid == None
        assert result.commit_date_string == 1
        assert result.rows_inserted == 2
        assert result.rows_updated == 3
        assert result.rows_deleted == 4
        assert result.commit_time == 5

    def test_get_conn_str_connstr(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")

        h_engine = hudi_engine.HudiEngine(
            feature_store_id=feature_store_id,
            feature_store_name=None,
            feature_group=None,
            spark_context=None,
            spark_session=None,
        )
        h_engine._connstr = "test_url;"

        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "1"
        )
        mock_client_get_instance.return_value._cert_key = "2"
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = "3"
        mock_client_get_instance.return_value._cert_key = "4"

        # Act
        result = (
            h_engine._get_conn_str()
        )  # todo this method assumes that _connstr ends with ";", but is it a fair assumption (since the generated results doesnt end with ";")?

        # Assert
        assert (
            result
            == "test_url;sslTrustStore=1;trustStorePassword=4;sslKeyStore=3;keyStorePassword=4"
        )
