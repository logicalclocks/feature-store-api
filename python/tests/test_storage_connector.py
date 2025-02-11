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

import base64
from pathlib import WindowsPath

import pytest
from hsfs import engine, storage_connector
from hsfs.engine import python, spark
from hsfs.storage_connector import BigQueryConnector


class TestHopsfsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_hopsfs"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_hopsfs"
        assert sc._featurestore_id == 67
        assert sc.description == "HOPSFS connector description"
        assert sc._hopsfs_path == "test_path"
        assert sc._dataset_name == "test_dataset_name"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_hopsfs_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_hopsfs"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc._hopsfs_path is None
        assert sc._dataset_name is None


class TestS3Connector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_s3"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_s3"
        assert sc._featurestore_id == 67
        assert sc.description == "S3 connector description"
        assert sc.access_key == "test_access_key"
        assert sc.secret_key == "test_secret_key"
        assert sc.server_encryption_algorithm == "test_server_encryption_algorithm"
        assert sc.server_encryption_key == "test_server_encryption_key"
        assert sc.bucket == "test_bucket"
        assert sc.session_token == "test_session_token"
        assert sc.iam_role == "test_iam_role"
        assert sc.arguments == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_s3_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_s3"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.access_key is None
        assert sc.secret_key is None
        assert sc.server_encryption_algorithm is None
        assert sc.server_encryption_key is None
        assert sc.bucket is None
        assert sc.session_token is None
        assert sc.iam_role is None
        assert sc.arguments == {}

    def test_default_path(self, mocker):
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mocker.patch(
            "hsfs.storage_connector.StorageConnector.refetch", return_value=None
        )
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")

        # act
        sc = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1, bucket="test-bucket"
        )
        sc.read(data_format="csv")
        # assert
        assert "s3://test-bucket" in mock_engine_read.call_args[0][3]


class TestRedshiftConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_redshift"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_redshift"
        assert sc._featurestore_id == 67
        assert sc.description == "Redshift connector description"
        assert sc.cluster_identifier == "test_cluster_identifier"
        assert sc.database_driver == "test_database_driver"
        assert sc.database_endpoint == "test_database_endpoint"
        assert sc.database_name == "test_database_name"
        assert sc.database_port == "test_database_port"
        assert sc.table_name == "test_table_name"
        assert sc.database_user_name == "test_database_user_name"
        assert sc.auto_create == "test_auto_create"
        assert sc.database_password == "test_database_password"
        assert sc.database_group == "test_database_group"
        assert sc.iam_role == "test_iam_role"
        assert sc.arguments == "test_arguments"
        assert sc.expiration == "test_expiration"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_redshift_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_redshift"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.cluster_identifier is None
        assert sc.database_driver is None
        assert sc.database_endpoint is None
        assert sc.database_name is None
        assert sc.database_port is None
        assert sc.table_name is None
        assert sc.database_user_name is None
        assert sc.auto_create is None
        assert sc.database_password is None
        assert sc.database_group is None
        assert sc.iam_role is None
        assert sc.arguments is None
        assert sc.expiration is None

    def test_read_query_option(self, mocker):
        sc = storage_connector.RedshiftConnector(
            id=1,
            name="redshiftconn",
            featurestore_id=1,
            table_name="abc",
            cluster_identifier="cluster",
            database_endpoint="us-east-2",
            database_port=5439,
            database_name="db",
        )

        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mocker.patch("hsfs.core.storage_connector_api.StorageConnectorApi.refetch")
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")

        query = "select * from table"
        sc.read(query=query)

        assert mock_engine_read.call_args[0][2].get("dbtable", None) is None
        assert mock_engine_read.call_args[0][2].get("query") == query


class TestAdlsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_adls"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_adls"
        assert sc._featurestore_id == 67
        assert sc.description == "Adls connector description"
        assert sc.generation == "test_generation"
        assert sc.directory_id == "test_directory_id"
        assert sc.application_id == "test_application_id"
        assert sc.service_credential == "test_service_credential"
        assert sc.account_name == "test_account_name"
        assert sc.container_name == "test_container_name"
        assert sc._spark_options == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_adls_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_adls"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.generation is None
        assert sc.directory_id is None
        assert sc.application_id is None
        assert sc.service_credential is None
        assert sc.account_name is None
        assert sc.container_name is None
        assert sc._spark_options == {}

    def test_default_path(self, mocker):
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")
        # act
        sc = storage_connector.AdlsConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            account_name="test_account",
            container_name="test_container",
            generation=2,
        )
        sc.read(data_format="csv")
        # assert read path value
        print(mock_engine_read.call_args[0])
        assert (
            "abfss://test_container@test_account.dfs.core.windows.net"
            in mock_engine_read.call_args[0][3]
        )


class TestSnowflakeConnector:
    def test_read_query_option(self, mocker):
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")

        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table="snowflake_table"
        )
        query = "select * from table"
        snowflake_connector.read(query=query)

        assert mock_engine_read.call_args[0][2].get("dbtable", None) is None
        assert mock_engine_read.call_args[0][2].get("query") == query

    def test_spark_options_db_table_none(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=None
        )

        spark_options = snowflake_connector.spark_options()

        assert "dbtable" not in spark_options

    def test_spark_options_db_table_empty(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=""
        )

        spark_options = snowflake_connector.spark_options()

        assert "dbtable" not in spark_options

    def test_spark_options_db_table_value(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table="test"
        )

        spark_options = snowflake_connector.spark_options()

        assert spark_options["dbtable"] == "test"

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_snowflake"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_snowflake"
        assert sc._featurestore_id == 67
        assert sc.description == "Snowflake connector description"
        assert sc.database == "test_database"
        assert sc.password == "test_password"
        assert sc.token == "test_token"
        assert sc.role == "test_role"
        assert sc.schema == "test_schema"
        assert sc.table == "test_table"
        assert sc.url == "test_url"
        assert sc.user == "test_user"
        assert sc.warehouse == "test_warehouse"
        assert sc.application == "test_application"
        assert sc._options == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_snowflake_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_snowflake"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.database is None
        assert sc.password is None
        assert sc.token is None
        assert sc.role is None
        assert sc.schema is None
        assert sc.table is None
        assert sc.url is None
        assert sc.user is None
        assert sc.warehouse is None
        assert sc.application is None
        assert sc._options == {}


class TestJdbcConnector:
    def test_spark_options_arguments_none(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments=None,
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string

    def test_spark_options_arguments_empty(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments="",
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string

    def test_spark_options_arguments_arguments(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )
        arguments = [
            {"name": "arg1", "value": "value1"},
            {"name": "arg2", "value": "value2"},
        ]

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments=arguments,
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string
        assert spark_options["arg1"] == "value1"
        assert spark_options["arg2"] == "value2"

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_jdbc"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_jdbc"
        assert sc._featurestore_id == 67
        assert sc.description == "JDBC connector description"
        assert sc.connection_string == "test_conn_string"
        assert sc.arguments == [
            {"name": "sslTrustStore"},
            {"name": "trustStorePassword"},
            {"name": "sslKeyStore"},
            {"name": "keyStorePassword"},
        ]

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_jdbc_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_jdbc"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.connection_string is None
        assert sc.arguments is None


class TestKafkaConnector:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_kafka"
        assert sc._featurestore_id == 67
        assert sc.description == "Kafka connector description"
        assert sc._bootstrap_servers == "test_bootstrap_servers"
        assert sc.security_protocol == "test_security_protocol"
        assert sc.ssl_truststore_location == "result_from_add_file"
        assert sc._ssl_truststore_password == "test_ssl_truststore_password"
        assert sc.ssl_keystore_location == "result_from_add_file"
        assert sc._ssl_keystore_password == "test_ssl_keystore_password"
        assert sc._ssl_key_password == "test_ssl_key_password"
        assert (
            sc.ssl_endpoint_identification_algorithm
            == "test_ssl_endpoint_identification_algorithm"
        )
        assert sc.options == {"test_option_name": "test_option_value"}

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_basic_info"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_kafka"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc._bootstrap_servers is None
        assert sc.security_protocol is None
        assert sc.ssl_truststore_location == "result_from_add_file"
        assert sc._ssl_truststore_password is None
        assert sc.ssl_keystore_location == "result_from_add_file"
        assert sc._ssl_keystore_password is None
        assert sc._ssl_key_password is None
        assert sc.ssl_endpoint_identification_algorithm is None
        assert sc.options == {}

    # Unit test for storage connector created by user (i.e. without the external flag)
    def test_kafka_options_user_sc(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.client.get_instance")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.kafka_options()

        # Assert
        assert config == {
            "test_option_name": "test_option_value",
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.truststore.location": "result_from_add_file",
            "ssl.truststore.password": "test_ssl_truststore_password",
            "ssl.keystore.location": "result_from_add_file",
            "ssl.keystore.password": "test_ssl_keystore_password",
            "ssl.key.password": "test_ssl_key_password",
        }

    def test_kafka_options_intenral(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_instance")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "result_from_get_jks_trust_store_path"
        )
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = (
            "result_from_get_jks_key_store_path"
        )
        mock_client_get_instance.return_value._cert_key = "result_from_cert_key"

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.kafka_options()

        # Assert
        assert config == {
            "test_option_name": "test_option_value",
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.truststore.location": "result_from_get_jks_trust_store_path",
            "ssl.truststore.password": "result_from_cert_key",
            "ssl.keystore.location": "result_from_get_jks_key_store_path",
            "ssl.keystore.password": "result_from_cert_key",
            "ssl.key.password": "result_from_cert_key",
        }

    def test_kafka_options_external(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.kafka_options()

        # Assert
        assert config == {
            "test_option_name": "test_option_value",
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.truststore.location": "result_from_add_file",
            "ssl.truststore.password": "test_ssl_truststore_password",
            "ssl.keystore.location": "result_from_add_file",
            "ssl.keystore.password": "test_ssl_keystore_password",
            "ssl.key.password": "test_ssl_key_password",
        }

    def test_spark_options(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"

        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "result_from_get_jks_trust_store_path"
        )
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = (
            "result_from_get_jks_key_store_path"
        )
        mock_client_get_instance.return_value._cert_key = "result_from_cert_key"

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.spark_options()

        # Assert
        assert config == {
            "kafka.test_option_name": "test_option_value",
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.truststore.location": "result_from_get_jks_trust_store_path",
            "kafka.ssl.truststore.password": "result_from_cert_key",
            "kafka.ssl.keystore.location": "result_from_get_jks_key_store_path",
            "kafka.ssl.keystore.password": "result_from_cert_key",
            "kafka.ssl.key.password": "result_from_cert_key",
        }

    def test_spark_options_external(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]

        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.spark_options()

        # Assert
        assert config == {
            "kafka.test_option_name": "test_option_value",
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.truststore.location": "result_from_add_file",
            "kafka.ssl.truststore.password": "test_ssl_truststore_password",
            "kafka.ssl.keystore.location": "result_from_add_file",
            "kafka.ssl.keystore.password": "test_ssl_keystore_password",
            "kafka.ssl.key.password": "test_ssl_key_password",
        }

    def test_spark_options_spark_35(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.5.0"

        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "result_from_get_jks_trust_store_path"
        )
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = (
            "result_from_get_jks_key_store_path"
        )
        mock_client_get_instance.return_value._cert_key = "result_from_cert_key"
        mock_client_get_instance.return_value._write_pem.return_value = (
            None,
            None,
            None,
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Mock the read pem method in the storage connector itself
        sc._read_pem = mocker.Mock()
        sc._read_pem.side_effect = [
            "test_ssl_ca",
            "test_ssl_certificate",
            "test_ssl_key",
        ]

        # Act
        config = sc.spark_options()

        # Assert
        assert config == {
            "kafka.test_option_name": "test_option_value",
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.truststore.type": "PEM",
            "kafka.ssl.keystore.type": "PEM",
            "kafka.ssl.truststore.certificates": "test_ssl_ca",
            "kafka.ssl.keystore.certificate.chain": "test_ssl_certificate",
            "kafka.ssl.keystore.key": "test_ssl_key",
        }

    def test_confluent_options(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        mock_client = mocker.patch("hsfs.client.get_instance")
        mock_client.return_value._write_pem.return_value = (
            "test_ssl_ca_location",
            "test_ssl_certificate_location",
            "test_ssl_key_location",
        )

        # Act
        config = sc.confluent_options()

        # Assert
        assert config == {
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.ca.location": "test_ssl_ca_location",
            "ssl.certificate.location": "test_ssl_certificate_location",
            "ssl.key.location": "test_ssl_key_location",
        }

    def test_confluent_options_jaas_single_quotes(self, mocker):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = None
        mocker.patch("hsfs.client.get_instance")
        sc = storage_connector.KafkaConnector(
            1,
            "kafka_connector",
            0,
            external_kafka=True,
            options=[
                {
                    "name": "sasl.jaas.config",
                    "value": "org.apache.kafka.common.security.plain.PlainLoginModule required username='222' password='111';",
                }
            ],
        )

        # Act
        config = sc.confluent_options()

        # Assert
        assert config == {
            "bootstrap.servers": None,
            "security.protocol": None,
            "ssl.endpoint.identification.algorithm": None,
            "sasl.mechanisms": "PLAIN",
            "sasl.password": "111",
            "sasl.username": "222",
        }

    def test_confluent_options_jaas_double_quotes(self, mocker):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = None
        mocker.patch("hsfs.client.get_instance")
        sc = storage_connector.KafkaConnector(
            1,
            "kafka_connector",
            0,
            external_kafka=True,
            options=[
                {
                    "name": "sasl.jaas.config",
                    "value": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="222" password="111";',
                }
            ],
        )

        # Act
        config = sc.confluent_options()

        # Assert
        assert config == {
            "bootstrap.servers": None,
            "security.protocol": None,
            "ssl.endpoint.identification.algorithm": None,
            "sasl.mechanisms": "PLAIN",
            "sasl.password": "111",
            "sasl.username": "222",
        }


class TestGcsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_gcs"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_gcs"
        assert sc._featurestore_id == 67
        assert sc.description == "Gcs connector description"
        assert sc.key_path == "test_key_path"
        assert sc.bucket == "test_bucket"
        assert sc.algorithm == "test_algorithm"
        assert sc.encryption_key == "test_encryption_key"
        assert sc.encryption_key_hash == "test_encryption_key_hash"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_gcs_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_gcs"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.key_path is None
        assert sc.bucket is None
        assert sc.algorithm is None
        assert sc.encryption_key is None
        assert sc.encryption_key_hash is None

    def test_python_support_validation(self, backend_fixtures):
        engine.set_instance("python", python.Engine())
        json = backend_fixtures["storage_connector"]["get_gcs_basic_info"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        with pytest.raises(NotImplementedError):
            sc.read()

    def test_default_path(self, mocker):
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")
        # act
        sc = storage_connector.GcsConnector(
            id=1, name="test_connector", featurestore_id=1, bucket="test-bucket"
        )
        sc.read(data_format="csv")
        # assert
        assert mock_engine_read.call_args[0][3] == "gs://test-bucket"


class TestBigQueryConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_big_query_table"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_big_query"
        assert sc._featurestore_id == 67
        assert sc.description == "BigQuery connector description"
        assert sc.key_path == "test_key_path"
        assert sc.parent_project == "test_parent_project"
        assert sc.dataset == "test_dataset"
        assert sc.query_table == "test_query_table"
        assert sc.query_project == "test_query_project"
        assert sc.arguments == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_big_query_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_big_query"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.key_path is None
        assert sc.parent_project is None
        assert sc.dataset is None
        assert sc.query_table is None
        assert sc.query_project is None
        assert sc.materialization_dataset is None
        assert sc.arguments == {}

    def test_credentials_base64_encoded(self, mocker, backend_fixtures, tmp_path):
        # Arrange
        engine.set_instance("spark", spark.Engine())
        mocker.patch("hsfs.client.get_instance")

        credentials = '{"type": "service_account", "project_id": "test"}'

        credentialsFile = tmp_path / "bigquery.json"
        credentialsFile.write_text(credentials)

        json = backend_fixtures["storage_connector"]["get_big_query_table"]["response"]
        if isinstance(tmp_path, WindowsPath):
            json["key_path"] = "file:///" + str(credentialsFile.resolve()).replace(
                "\\", "/"
            )
        else:
            json["key_path"] = "file://" + str(credentialsFile.resolve())

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        spark_options = sc.spark_options()

        # Assert - Credentials should be base64 encoded
        assert (
            base64.b64decode(spark_options[BigQueryConnector.BIGQ_CREDENTIALS]).decode(
                "utf-8"
            )
            == credentials
        )

    def test_python_support_validation(self, backend_fixtures):
        # Arrange
        engine.set_instance("python", python.Engine())
        json = backend_fixtures["storage_connector"]["get_big_query_basic_info"][
            "response"
        ]
        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)
        # Assert
        with pytest.raises(NotImplementedError):
            sc.read()

    def test_query_validation(self, mocker, backend_fixtures, tmp_path):
        # Arrange
        engine.set_instance("spark", spark.Engine())
        mocker.patch("hsfs.client.get_instance")

        credentials = '{"type": "service_account", "project_id": "test"}'
        credentialsFile = tmp_path / "bigquery.json"
        credentialsFile.write_text(credentials)
        json = backend_fixtures["storage_connector"]["get_big_query_table"]["response"]
        if isinstance(tmp_path, WindowsPath):
            json["key_path"] = "file:///" + str(credentialsFile.resolve()).replace(
                "\\", "/"
            )
        else:
            json["key_path"] = "file://" + str(credentialsFile.resolve())
        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)
        # Assert
        with pytest.raises(ValueError):
            sc.read(query="select * from")

    def test_connector_options(self, backend_fixtures):
        # Arrange
        engine.set_instance("python", python.Engine())
        json = backend_fixtures["storage_connector"]["get_big_query_query"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        options = sc.connector_options()

        # Assert
        assert options["project_id"] == "test_parent_project"
