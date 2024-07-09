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
import importlib

from hsfs import storage_connector
from hsfs.core import constants, kafka_engine


if constants.HAS_CONFLUENT_KAFKA:
    from confluent_kafka.admin import PartitionMetadata, TopicMetadata


class TestKafkaEngine:
    def test_kafka_produce(self, mocker):
        # Arrange
        producer = mocker.Mock()

        # Act
        kafka_engine.kafka_produce(
            producer=producer,
            topic_name="test_topic",
            headers={},
            key=None,
            encoded_row=None,
            acked=None,
            debug_kafka=False,
        )

        # Assert
        assert producer.produce.call_count == 1
        assert producer.poll.call_count == 1

    def test_kafka_produce_buffer_error(self, mocker):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_print = mocker.patch("builtins.print")

        producer = mocker.Mock()
        producer.produce.side_effect = [BufferError("test_error"), None]
        # Act
        kafka_engine.kafka_produce(
            producer=producer,
            topic_name="test_topic",
            headers={},
            key=None,
            encoded_row=None,
            acked=None,
            debug_kafka=True,
        )

        # Assert
        assert producer.produce.call_count == 2
        assert producer.poll.call_count == 2
        assert mock_print.call_count == 1
        assert mock_print.call_args[0][0] == "Caught: test_error"

    def test_encode_complex_features(self):
        # Arrange
        def test_utf(value, bytes_io):
            bytes_io.write(bytes(value, "utf-8"))

        # Act
        result = kafka_engine.encode_complex_features(
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
        constants.HAS_AVRO = True
        constants.HAS_FAST_AVRO = False
        importlib.reload(kafka_engine)

        # Act
        result = kafka_engine.get_encoder_func(
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
        mock_avro_schema_parse = mocker.patch("avro.schema.parse")
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
        constants.HAS_AVRO = False
        constants.HAS_FAST_AVRO = True
        importlib.reload(kafka_engine)

        # Act
        result = kafka_engine.get_encoder_func(
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

        mocker.patch("hsfs.core.kafka_engine.isinstance", return_value=True)

        mock_client = mocker.patch("hsfs.client.get_instance")
        mock_client.return_value._write_pem.return_value = (
            "test_ssl_ca_location",
            "test_ssl_certificate_location",
            "test_ssl_key_location",
        )

        # Act
        result = kafka_engine.get_kafka_config(
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

        # Act
        result = kafka_engine.get_kafka_config(
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

        # Act
        result = kafka_engine.get_kafka_config(
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

        # Act
        result = kafka_engine.get_kafka_config(
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

    def test_kafka_get_offsets_high(self, mocker):
        # Arrange
        feature_store_id = 99
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
            "hsfs.core.kafka_engine.init_kafka_consumer",
            return_value=consumer,
        )

        # Act
        result = kafka_engine.kafka_get_offsets(
            topic_name=topic_name,
            feature_store_id=feature_store_id,
            offline_write_options={},
            high=True,
        )

        # Assert
        assert result == f" -initialCheckPointString {topic_name},0:11"

    def test_kafka_get_offsets_low(self, mocker):
        # Arrange
        feature_store_id = 99
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
            "hsfs.core.kafka_engine.init_kafka_consumer",
            return_value=consumer,
        )

        # Act
        result = kafka_engine.kafka_get_offsets(
            feature_store_id=feature_store_id,
            topic_name=topic_name,
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
            "hsfs.core.kafka_engine.init_kafka_consumer",
            return_value=consumer,
        )
        # Act
        result = kafka_engine.kafka_get_offsets(
            topic_name=topic_name,
            feature_store_id=99,
            offline_write_options={},
            high=True,
        )

        # Assert
        assert result == ""

    def test_spark_get_kafka_config(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )
        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        mocker.patch("hsfs.core.kafka_engine.isinstance", return_value=True)

        # Act
        results = kafka_engine.get_kafka_config(
            1, write_options={"user_opt": "ABC"}, engine="spark"
        )

        # Assert
        assert results == {
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.key.password": "test_ssl_key_password",
            "kafka.ssl.keystore.location": "result_from_add_file",
            "kafka.ssl.keystore.password": "test_ssl_keystore_password",
            "kafka.ssl.truststore.location": "result_from_add_file",
            "kafka.ssl.truststore.password": "test_ssl_truststore_password",
            "kafka.test_option_name": "test_option_value",
            "user_opt": "ABC",
        }
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_count == 1
        )
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_args[0][1]
            is False
        )

    def test_spark_get_kafka_config_external_client(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )
        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        # Act
        results = kafka_engine.get_kafka_config(
            1, write_options={"user_opt": "ABC"}, engine="spark"
        )

        # Assert
        assert results == {
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.key.password": "test_ssl_key_password",
            "kafka.ssl.keystore.location": "result_from_add_file",
            "kafka.ssl.keystore.password": "test_ssl_keystore_password",
            "kafka.ssl.truststore.location": "result_from_add_file",
            "kafka.ssl.truststore.password": "test_ssl_truststore_password",
            "kafka.test_option_name": "test_option_value",
            "user_opt": "ABC",
        }
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_count == 1
        )
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_args[0][1]
            is True
        )

    def test_spark_get_kafka_config_internal_kafka(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )
        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        # Act
        results = kafka_engine.get_kafka_config(
            1, write_options={"user_opt": "ABC", "internal_kafka": True}, engine="spark"
        )

        # Assert
        assert results == {
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.key.password": "test_ssl_key_password",
            "kafka.ssl.keystore.location": "result_from_add_file",
            "kafka.ssl.keystore.password": "test_ssl_keystore_password",
            "kafka.ssl.truststore.location": "result_from_add_file",
            "kafka.ssl.truststore.password": "test_ssl_truststore_password",
            "kafka.test_option_name": "test_option_value",
            "user_opt": "ABC",
            "internal_kafka": True,
        }
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_count == 1
        )
        assert (
            mock_storage_connector_api.return_value.get_kafka_connector.call_args[0][1]
            is False
        )
