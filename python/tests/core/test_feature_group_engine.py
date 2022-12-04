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

import pytest

from hsfs import feature_group, feature_group_commit, validation_report, feature
from hsfs.client import exceptions
from hsfs.core import feature_group_engine


class TestFeatureGroupEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.save(
            feature_group=fg,
            feature_dataframe=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1

    def test_save_ge_report(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mock_ge_engine = mocker.patch(
            "hsfs.core.great_expectation_engine.GreatExpectationEngine"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        vr = validation_report.ValidationReport(
            success=None,
            results=[],
            meta=None,
            statistics=None,
            ingestion_result="REJECTED",
        )

        mock_ge_engine.return_value.validate.return_value = vr

        # Act
        fg_engine.save(
            feature_group=fg,
            feature_dataframe=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0

    def test_insert(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        # Act
        fg_engine.insert(
            feature_group=fg,
            feature_dataframe=None,
            overwrite=None,
            operation=None,
            storage=None,
            write_options=None,
        )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1

    def test_insert_id(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.insert(
            feature_group=fg,
            feature_dataframe=None,
            overwrite=None,
            operation=None,
            storage=None,
            write_options=None,
        )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1

    def test_insert_ge_report(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_ge_engine = mocker.patch(
            "hsfs.core.great_expectation_engine.GreatExpectationEngine"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        vr = validation_report.ValidationReport(
            success=None,
            results=[],
            meta=None,
            statistics=None,
            ingestion_result="REJECTED",
        )

        mock_ge_engine.return_value.validate.return_value = vr

        # Act
        fg_engine.insert(
            feature_group=fg,
            feature_dataframe=None,
            overwrite=None,
            operation=None,
            storage=None,
            write_options=None,
        )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0

    def test_insert_storage(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine.insert(
                feature_group=fg,
                feature_dataframe=None,
                overwrite=None,
                operation=None,
                storage="online",
                write_options=None,
            )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            str(e_info.value) == "Online storage is not enabled for this feature group."
        )

    def test_insert_overwrite(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        # Act
        fg_engine.insert(
            feature_group=fg,
            feature_dataframe=None,
            overwrite=True,
            operation=None,
            storage=None,
            write_options=None,
        )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 1
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1

    def test_delete(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.delete(feature_group=fg)

        # Assert
        assert mock_fg_api.return_value.delete.call_count == 1

    def test_commit_details(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine.commit_details(feature_group=fg, wallclock_time=None, limit=None)

        # Assert
        assert mock_fg_api.return_value.get_commit_details.call_count == 0
        assert (
            str(e_info.value)
            == "commit_details can only be used on time travel enabled feature groups"
        )

    def test_commit_details_time_travel_format(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            time_travel_format="wrong",
            id=10,
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine.commit_details(feature_group=fg, wallclock_time=None, limit=None)

        # Assert
        assert mock_fg_api.return_value.get_commit_details.call_count == 0
        assert (
            str(e_info.value)
            == "commit_details can only be used on time travel enabled feature groups"
        )

    def test_commit_details_time_travel_format_hudi(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            time_travel_format="HUDI",
            id=10,
        )

        # Act
        fg_engine.commit_details(feature_group=fg, wallclock_time=None, limit=None)

        # Assert
        assert mock_fg_api.return_value.get_commit_details.call_count == 1

    def test_commit_details_time_travel_format_hudi_fg_commit(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_util_get_hudi_datestr_from_timestamp = mocker.patch(
            "hsfs.util.get_hudi_datestr_from_timestamp"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            time_travel_format="HUDI",
            id=10,
        )

        fg_commit = feature_group_commit.FeatureGroupCommit(
            commitid=1, rows_inserted=2, rows_updated=3, rows_deleted=4
        )
        mock_fg_api.return_value.get_commit_details.return_value = [fg_commit]
        mock_util_get_hudi_datestr_from_timestamp.return_value = "123"

        # Act
        result = fg_engine.commit_details(
            feature_group=fg, wallclock_time=None, limit=None
        )

        # Assert
        assert mock_fg_api.return_value.get_commit_details.call_count == 1
        assert result == {
            1: {
                "committedOn": "123",
                "rowsUpdated": 3,
                "rowsInserted": 2,
                "rowsDeleted": 4,
            }
        }

    def test_commit_delete(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.commit_delete(feature_group=fg, delete_df=None, write_options=None)

        # Assert
        assert mock_hudi_engine.return_value.delete_record.call_count == 1

    def test_sql(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_sc_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch("hsfs.engine.get_instance")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_engine.sql(
            query=None,
            feature_store_name=None,
            dataframe_type=None,
            online=None,
            read_options=None,
        )

        # Assert
        assert mock_sc_api.return_value.get_online_connector.call_count == 0

    def test_sql_online(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_sc_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch("hsfs.engine.get_instance")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_engine.sql(
            query=None,
            feature_store_name=None,
            dataframe_type=None,
            online=True,
            read_options=None,
        )

        # Assert
        assert mock_sc_api.return_value.get_online_connector.call_count == 1

    def test_update_features_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine._update_features_metadata(feature_group=fg, features=None)

        # Assert
        assert mock_fg_api.return_value.update_metadata.call_count == 1

    def test_update_features(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_engine_new_feature_list = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.new_feature_list"
        )
        mock_fg_engine_update_features_metadata = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._update_features_metadata"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_engine.update_features(feature_group=None, updated_features=None)

        # Assert
        assert mock_fg_engine_new_feature_list.call_count == 1
        assert mock_fg_engine_update_features_metadata.call_count == 1

    def test_append_features(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_engine_update_features_metadata = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._update_features_metadata"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="str")
        f2 = feature.Feature(name="f2", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[f, f1],
            id=10,
        )

        fg.read = mocker.Mock()

        # Act
        fg_engine.append_features(feature_group=fg, new_features=[f1, f2])

        # Assert
        assert (
            mock_engine_get_instance.return_value.save_empty_dataframe.call_count == 1
        )
        assert len(mock_fg_engine_update_features_metadata.call_args[0][1]) == 4

    def test_update_description(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.update_description(feature_group=fg, description=None)

        # Assert
        assert mock_fg_api.return_value.update_metadata.call_count == 1

    def test_get_subject(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_kafka_api = mocker.patch("hsfs.core.kafka_api.KafkaApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.get_subject(feature_group=fg)

        # Assert
        assert mock_kafka_api.return_value.get_topic_subject.call_count == 1

    def test_get_kafka_config(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_kafka_api = mocker.patch("hsfs.core.kafka_api.KafkaApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        mock_kafka_api.return_value.get_broker_endpoints.return_value = ["url1", "url2"]
        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "1"
        )
        mock_client_get_instance.return_value._cert_key = "2"
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = "3"
        mock_client_get_instance.return_value._cert_key = "4"

        # Act
        results = fg_engine.get_kafka_config(online_write_options={})

        # Assert
        assert results == {
            "kafka.bootstrap.servers": "url1,url2",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.endpoint.identification.algorithm": "",
            "kafka.ssl.key.password": "4",
            "kafka.ssl.keystore.location": "3",
            "kafka.ssl.keystore.password": "4",
            "kafka.ssl.truststore.location": "1",
            "kafka.ssl.truststore.password": "4",
        }
        assert mock_kafka_api.return_value.get_broker_endpoints.call_count == 1

    def test_insert_stream(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine.insert_stream(
                feature_group=fg,
                dataframe=None,
                query_name=None,
                output_mode=None,
                await_termination=None,
                timeout=None,
                checkpoint_dir=None,
                write_options=None,
            )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 0
        )
        assert (
            str(e_info.value)
            == "Online storage is not enabled for this feature group. It is currently only possible to stream to the online storage."
        )

    def test_insert_stream_online_enabled(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_warnings_warn = mocker.patch("warnings.warn")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            online_enabled=True,
        )

        # Act
        fg_engine.insert_stream(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode=None,
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 1
        )
        assert (
            mock_warnings_warn.call_args[0][0]
            == "`insert_stream` method in the next release will be available only for feature groups created with "
            "`stream=True`."
        )

    def test_insert_stream_stream(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            stream=True,
        )

        # Act
        fg_engine.insert_stream(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode=None,
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 1
        )

    def test_insert_stream_online_enabled_id(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.get_kafka_config"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_warnings_warn = mocker.patch("warnings.warn")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            online_enabled=True,
            id=10,
        )

        # Act
        fg_engine.insert_stream(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode=None,
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 1
        )
        assert (
            mock_warnings_warn.call_args[0][0]
            == "`insert_stream` method in the next release will be available only for feature groups created with "
            "`stream=True`."
        )

    def test_verify_schema_compatibility(self):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg_features = []
        df_features = []

        # Act
        fg_engine._verify_schema_compatibility(
            feature_group_features=fg_features, dataframe_features=df_features
        )

        # Assert
        assert len(fg_features) == 0
        assert len(df_features) == 0

    def test_verify_schema_compatibility_feature_group_features(self):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="int")

        fg_features = [f, f1]
        df_features = []

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine._verify_schema_compatibility(
                feature_group_features=fg_features, dataframe_features=df_features
            )

        # Assert
        assert len(fg_features) == 2
        assert len(df_features) == 0
        assert (
            str(e_info.value)
            == "Features are not compatible with Feature Group schema: \n"
            " - f (type: 'str') is missing from input dataframe.\n"
            " - f1 (type: 'int') is missing from input dataframe."
        )

    def test_verify_schema_compatibility_dataframe_features(self):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="int")

        fg_features = []
        df_features = [f, f1]

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine._verify_schema_compatibility(
                feature_group_features=fg_features, dataframe_features=df_features
            )

        # Assert
        assert len(fg_features) == 0
        assert len(df_features) == 2
        assert (
            str(e_info.value)
            == "Features are not compatible with Feature Group schema: \n"
            " - f (type: 'str') does not exist in feature group.\n"
            " - f1 (type: 'int') does not exist in feature group."
        )

    def test_verify_schema_compatibility_feature_group_features_dataframe_features(
        self,
    ):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="int  ")
        f1_2 = feature.Feature(name="f1", type="INT")

        fg_features = [f, f1]
        df_features = [f, f1_2]

        # Act
        fg_engine._verify_schema_compatibility(
            feature_group_features=fg_features, dataframe_features=df_features
        )

        # Assert
        assert len(fg_features) == 2
        assert len(df_features) == 2

    def test_verify_schema_compatibility_feature_group_features_dataframe_features_wrong_type(
        self,
    ):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="int")
        f1_2 = feature.Feature(name="f1", type="bool")

        fg_features = [f, f1]
        df_features = [f, f1_2]

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine._verify_schema_compatibility(
                feature_group_features=fg_features, dataframe_features=df_features
            )

        # Assert
        assert len(fg_features) == 2
        assert len(df_features) == 2
        assert (
            str(e_info.value)
            == "Features are not compatible with Feature Group schema: \n"
            " - f1 (expected type: 'int', derived from input: 'bool') has the wrong type."
        )

    def test_save_feature_group_metadata(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine._save_feature_group_metadata(
            feature_group=fg, dataframe_features=[f], write_options=None
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert f.primary is False
        assert f.partition is False
        assert f.hudi_precombine_key is False
        assert mock_print.call_count == 1
        assert mock_print.call_args[0][
            0
        ] == "Feature Group created successfully, explore it at \n{}".format(
            feature_group_url
        )

    def test_save_feature_group_metadata_features(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[f],
            id=10,
        )

        # Act
        fg_engine._save_feature_group_metadata(
            feature_group=fg, dataframe_features=None, write_options=None
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert f.primary is False
        assert f.partition is False
        assert f.hudi_precombine_key is False
        assert mock_print.call_count == 1
        assert mock_print.call_args[0][
            0
        ] == "Feature Group created successfully, explore it at \n{}".format(
            feature_group_url
        )

    def test_save_feature_group_metadata_primary_partition_precombine(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=["f"],
            partition_key=["f"],
            hudi_precombine_key="f",
            time_travel_format="HUDI",
        )

        # Act
        fg_engine._save_feature_group_metadata(
            feature_group=fg, dataframe_features=[f], write_options=None
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert f.primary is True
        assert f.partition is True
        assert f.hudi_precombine_key is True
        assert mock_print.call_count == 1
        assert mock_print.call_args[0][
            0
        ] == "Feature Group created successfully, explore it at \n{}".format(
            feature_group_url
        )

    def test_save_feature_group_metadata_primary_partition_precombine_event_error(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )

        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._get_feature_group_url",
            return_value=feature_group_url,
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_pk_info:
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["feature_name"],
                partition_key=["f"],
                hudi_precombine_key="f",
                event_time="f",
                time_travel_format="HUDI",
            )

            fg_engine._save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        with pytest.raises(exceptions.FeatureStoreException) as e_partk_info:
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["f"],
                partition_key=["feature_name"],
                hudi_precombine_key="f",
                event_time="f",
                time_travel_format="HUDI",
            )

            fg_engine._save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        with pytest.raises(exceptions.FeatureStoreException) as e_prekk_info:
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["f"],
                partition_key=["f"],
                hudi_precombine_key="feature_name",
                event_time="f",
                time_travel_format="HUDI",
            )

            fg_engine._save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        with pytest.raises(exceptions.FeatureStoreException) as e_eventt_info:
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["f"],
                partition_key=["f"],
                hudi_precombine_key="f",
                event_time="feature_name",
                time_travel_format="HUDI",
            )

            fg_engine._save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        assert (
            str(e_pk_info.value)
            == "Provided primary key(s) feature_name doesn't exist in feature dataframe"
        )
        assert (
            str(e_partk_info.value)
            == "Provided partition key(s) feature_name doesn't exist in feature dataframe"
        )
        assert (
            str(e_prekk_info.value)
            == "Provided hudi precombine key feature_name doesn't exist in feature "
            "dataframe"
        )
        assert (
            str(e_eventt_info.value)
            == "Provided event_time feature feature_name doesn't exist in feature dataframe"
        )

    def test_save_feature_group_metadata_write_options(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"
        write_options = {"spark": "test"}

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            stream=True,
            id=10,
        )

        # Act
        fg_engine._save_feature_group_metadata(
            feature_group=fg, dataframe_features=[f], write_options=write_options
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert f.primary is False
        assert f.partition is False
        assert f.hudi_precombine_key is False
        assert mock_print.call_count == 1
        assert mock_print.call_args[0][
            0
        ] == "Feature Group created successfully, explore it at \n{}".format(
            feature_group_url
        )

    def test_get_feature_group_url(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_util_get_hostname_replaced_url = mocker.patch(
            "hsfs.util.get_hostname_replaced_url"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        mock_client_get_instance.return_value._project_id = 50

        # Act
        fg_engine._get_feature_group_url(feature_group=fg)

        # Assert
        assert mock_util_get_hostname_replaced_url.call_count == 1
        assert (
            mock_util_get_hostname_replaced_url.call_args[0][0] == "/p/50/fs/99/fg/10"
        )
